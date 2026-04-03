"""Agricultural sector competitiveness analysis.

Combines Revealed Comparative Advantage (RCA) analysis with Domestic Resource
Cost (DRC) estimation to assess a country's competitive position in
agricultural trade.

1. **RCA (Balassa 1965)**:
    RCA_ij = (X_ij / X_i) / (X_wj / X_w)
    where X_ij = country i's exports of product j, X_i = total exports,
    X_wj = world exports of j, X_w = total world exports.
    RCA > 1 indicates revealed comparative advantage.

2. **Domestic Resource Cost (DRC)**:
    DRC_j = (sum of domestic factor costs) / (value added at border prices)
    DRC < 1: efficient use of domestic resources (competitive)
    DRC > 1: uses more domestic resources than the good is worth (uncompetitive)
    DRC = shadow exchange rate when DRC = 1 (Bruno 1972).

3. **Comparative advantage dynamics**:
    Track RCA evolution over time. Compute transition matrices for movement
    between RCA categories. Test for persistence (Markov chain stationarity).

Score (0-100): Higher score indicates declining agricultural competitiveness
(falling RCA, rising DRC).

References:
    Balassa, B. (1965). "Trade liberalisation and revealed comparative
        advantage." Manchester School, 33(2), 99-123.
    Bruno, M. (1972). "Domestic resource costs and effective protection."
        Journal of Political Economy, 80(1), 16-33.
    Fertoe, I., Hubbard, L.J. (2003). "Revealed comparative advantage and
        competitiveness in Hungarian agri-food sectors." World Economy.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class AgriculturalCompetitiveness(LayerBase):
    layer_id = "l5"
    name = "Agricultural Competitiveness"

    AG_SECTORS = (
        "cereals", "vegetables", "fruits", "oilseeds", "sugar",
        "meat", "dairy", "fish", "fibers", "beverages",
    )

    async def compute(self, db, **kwargs) -> dict:
        """Compute agricultural competitiveness indicators.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code
            year : int - reference year
            sectors : tuple - agricultural sectors to analyze
        """
        country = kwargs.get("country_iso3", "BGD")
        year = kwargs.get("year")
        sectors = kwargs.get("sectors", self.AG_SECTORS)

        # Fetch country agricultural exports by sector
        country_exports = await self._fetch_sector_exports(db, country, year)
        # Fetch world agricultural exports by sector
        world_exports = await self._fetch_sector_exports(db, "WLD", year)

        # Total country exports
        total_country_row = await db.fetch_one(
            """
            SELECT SUM(dp.value) as total
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source IN ('comtrade', 'baci')
              AND ds.description LIKE '%export%total%'
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )
        total_country_exp = total_country_row["total"] if total_country_row and total_country_row["total"] else None

        # Total world exports
        total_world_row = await db.fetch_one(
            """
            SELECT SUM(dp.value) as total
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = 'WLD'
              AND ds.source IN ('comtrade', 'baci')
              AND ds.description LIKE '%export%total%'
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (),
        )
        total_world_exp = total_world_row["total"] if total_world_row and total_world_row["total"] else None

        # Compute RCA for each sector
        rca_results = {}
        rca_values = []

        for sector in sectors:
            x_ij = country_exports.get(sector, 0)
            x_wj = world_exports.get(sector, 0)

            if total_country_exp and total_world_exp and total_country_exp > 0 and total_world_exp > 0 and x_wj > 0:
                rca = (x_ij / total_country_exp) / (x_wj / total_world_exp)
            else:
                rca = None

            rca_results[sector] = {
                "rca": round(rca, 4) if rca is not None else None,
                "has_advantage": rca > 1.0 if rca is not None else None,
                "country_export_value": x_ij,
                "world_export_value": x_wj,
            }
            if rca is not None:
                rca_values.append(rca)

        # RCA dynamics: fetch multi-year data for trend analysis
        rca_trends = await self._compute_rca_trends(db, country, sectors)

        # DRC analysis (if domestic cost data available)
        drc_results = await self._compute_drc(db, country, year, sectors)

        # Transition matrix for RCA categories
        transition_matrix = self._compute_transition_matrix(rca_trends)

        if not rca_values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient export data for RCA computation",
            }

        # Aggregate competitiveness metrics
        n_advantage = sum(1 for r in rca_values if r > 1.0)
        mean_rca = float(np.mean(rca_values))
        median_rca = float(np.median(rca_values))

        # Score: low agricultural competitiveness -> high score
        # Few sectors with RCA > 1, low mean RCA, rising DRC
        advantage_share = n_advantage / len(rca_values)
        rca_component = max(0, min(50, (1.0 - advantage_share) * 50))
        # Mean RCA: RCA < 0.5 is weak, normalize
        rca_level_component = max(0, min(30, (1.0 - min(mean_rca, 2.0) / 2.0) * 30))
        # DRC component (if available)
        drc_component = 0.0
        if drc_results:
            drc_vals = [v["drc"] for v in drc_results.values() if v.get("drc") is not None]
            if drc_vals:
                mean_drc = float(np.mean(drc_vals))
                drc_component = max(0, min(20, (mean_drc - 0.5) * 40))

        score = rca_component + rca_level_component + drc_component

        return {
            "score": round(max(0.0, min(100.0, score)), 2),
            "country": country,
            "year": year,
            "rca_by_sector": rca_results,
            "n_sectors_with_advantage": n_advantage,
            "n_sectors_analyzed": len(rca_values),
            "mean_rca": round(mean_rca, 4),
            "median_rca": round(median_rca, 4),
            "rca_trends": rca_trends,
            "drc_results": drc_results,
            "transition_matrix": transition_matrix,
        }

    async def _fetch_sector_exports(self, db, country: str, year: int | None) -> dict:
        """Fetch export values by agricultural sector."""
        year_clause = "AND dp.date = ?" if year else ""
        results = {}

        for sector in self.AG_SECTORS:
            params = [country, f"%{sector}%export%"]
            if year:
                params.append(str(year))
            row = await db.fetch_one(
                f"""
                SELECT dp.value
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.name LIKE ?
                  AND ds.source IN ('comtrade', 'baci', 'fao')
                  {year_clause}
                ORDER BY dp.date DESC
                LIMIT 1
                """,
                tuple(params),
            )
            results[sector] = row["value"] if row else 0.0

        return results

    async def _compute_rca_trends(self, db, country: str, sectors: tuple) -> dict:
        """Compute RCA over multiple years to assess dynamics."""
        trends = {}
        for sector in sectors:
            rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.name LIKE ?
                  AND ds.source IN ('comtrade', 'baci', 'fao')
                ORDER BY dp.date ASC
                """,
                (country, f"%{sector}%rca%"),
            )
            if len(rows) >= 3:
                dates = [r["date"] for r in rows]
                values = np.array([r["value"] for r in rows], dtype=float)
                # Linear trend: sign and magnitude
                t = np.arange(len(values), dtype=float)
                if np.std(t) > 0 and np.std(values) > 0:
                    slope = float(np.polyfit(t, values, 1)[0])
                else:
                    slope = 0.0
                trends[sector] = {
                    "start": {"date": dates[0], "rca": round(float(values[0]), 4)},
                    "end": {"date": dates[-1], "rca": round(float(values[-1]), 4)},
                    "trend_slope": round(slope, 6),
                    "direction": "improving" if slope > 0.01 else ("declining" if slope < -0.01 else "stable"),
                    "n_years": len(values),
                }
        return trends

    async def _compute_drc(self, db, country: str, year: int | None, sectors: tuple) -> dict:
        """Compute Domestic Resource Cost ratios.

        DRC = domestic factor cost (at shadow prices) / value added (at border prices)
        """
        drc_results = {}
        year_clause = "AND dp.date = ?" if year else ""

        for sector in sectors:
            # Domestic factor cost
            params_d = [country, f"%{sector}%domestic%factor%cost%"]
            if year:
                params_d.append(str(year))
            cost_row = await db.fetch_one(
                f"""
                SELECT dp.value
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.name LIKE ?
                  {year_clause}
                ORDER BY dp.date DESC LIMIT 1
                """,
                tuple(params_d),
            )

            # Value added at border prices
            params_v = [country, f"%{sector}%value%added%border%"]
            if year:
                params_v.append(str(year))
            va_row = await db.fetch_one(
                f"""
                SELECT dp.value
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.name LIKE ?
                  {year_clause}
                ORDER BY dp.date DESC LIMIT 1
                """,
                tuple(params_v),
            )

            if cost_row and va_row and va_row["value"] > 0:
                drc = cost_row["value"] / va_row["value"]
                drc_results[sector] = {
                    "drc": round(drc, 4),
                    "competitive": drc < 1.0,
                    "domestic_factor_cost": cost_row["value"],
                    "value_added_border": va_row["value"],
                }

        return drc_results

    @staticmethod
    def _compute_transition_matrix(rca_trends: dict) -> dict | None:
        """Compute Markov transition matrix for RCA categories.

        Categories: disadvantage (RCA < 0.5), weak (0.5-1.0),
        moderate (1.0-2.0), strong (RCA > 2.0).
        """
        if not rca_trends:
            return None

        categories = ["disadvantage", "weak", "moderate", "strong"]

        def classify(rca: float) -> int:
            if rca < 0.5:
                return 0
            elif rca < 1.0:
                return 1
            elif rca < 2.0:
                return 2
            else:
                return 3

        transitions = np.zeros((4, 4), dtype=float)
        for sector, trend in rca_trends.items():
            start_cat = classify(trend["start"]["rca"])
            end_cat = classify(trend["end"]["rca"])
            transitions[start_cat, end_cat] += 1

        # Normalize rows to get probabilities
        row_sums = transitions.sum(axis=1, keepdims=True)
        row_sums[row_sums == 0] = 1  # avoid division by zero
        probs = transitions / row_sums

        return {
            "categories": categories,
            "matrix": [[round(float(probs[i, j]), 3) for j in range(4)] for i in range(4)],
            "n_transitions": int(transitions.sum()),
        }
