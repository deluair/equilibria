"""Land use change analysis for agricultural systems.

Quantifies the dynamics of agricultural land expansion/contraction,
deforestation rates, and land use transitions. Tests the Environmental
Kuznets Curve (EKC) hypothesis for land use, which posits an inverted-U
relationship between income and environmental degradation: land clearing
accelerates during early development, then slows and reverses as countries
grow wealthier.

Components:
    1. **Land use transition matrix**: Markov chain of transitions between
       land categories (forest, cropland, pasture, urban, other) using
       consecutive period data.

    2. **Agricultural frontier dynamics**: Rate of cropland expansion,
       intensification vs extensification decomposition.

    3. **Deforestation rate**: Annual forest loss as % of total forest area.

    4. **EKC estimation**: Quadratic regression of deforestation/land
       clearing on GDP per capita:
       D_t = b0 + b1*Y_t + b2*Y_t^2 + e_t
       EKC turning point at Y* = -b1 / (2*b2).

Score (0-100): Higher score indicates rapid/unsustainable land use change
(high deforestation, rapid cropland expansion, pre-EKC-turning-point).

References:
    Lambin, E.F. et al. (2001). "The causes of land-use and land-cover
        change." Global Environmental Change, 11(4), 261-269.
    Barbier, E.B. (1997). "Introduction to the environmental Kuznets
        curve special issue." Environment and Development Economics.
    Mather, A.S. (1992). "The forest transition." Area, 24(4), 367-379.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class LandUseChange(LayerBase):
    layer_id = "l5"
    name = "Land Use Change"

    LAND_CATEGORIES = ("forest", "cropland", "pasture", "urban", "other")

    async def compute(self, db, **kwargs) -> dict:
        """Compute land use change indicators.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code
        """
        country = kwargs.get("country_iso3", "BGD")

        # Fetch land use time series by category
        land_series = {}
        for category in self.LAND_CATEGORIES:
            rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.source IN ('fao', 'wb', 'gfc')
                  AND ds.name LIKE ?
                ORDER BY dp.date ASC
                """,
                (country, f"%{category}%area%"),
            )
            if rows:
                land_series[category] = {
                    "dates": [r["date"] for r in rows],
                    "values": np.array([r["value"] for r in rows], dtype=float),
                }

        if len(land_series) < 2:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient land use data (need >= 2 categories)",
            }

        # 1. Compute change rates for each category
        change_rates = {}
        for cat, data in land_series.items():
            vals = data["values"]
            if len(vals) >= 2 and vals[0] > 0:
                total_change = (vals[-1] - vals[0]) / vals[0] * 100.0
                n_years = len(vals) - 1
                annual_rate = total_change / n_years if n_years > 0 else 0.0
                change_rates[cat] = {
                    "total_change_pct": round(float(total_change), 4),
                    "annual_rate_pct": round(float(annual_rate), 4),
                    "start_value": round(float(vals[0]), 2),
                    "end_value": round(float(vals[-1]), 2),
                    "n_years": n_years,
                }

        # 2. Deforestation rate
        deforestation_rate = None
        if "forest" in change_rates:
            deforestation_rate = -change_rates["forest"]["annual_rate_pct"]
            # Positive deforestation_rate means forest loss

        # 3. Cropland expansion
        cropland_expansion = None
        if "cropland" in change_rates:
            cropland_expansion = change_rates["cropland"]["annual_rate_pct"]

        # 4. Land use transition matrix
        transition_matrix = self._compute_transition_matrix(land_series)

        # 5. EKC estimation
        ekc_result = await self._estimate_ekc(db, country, land_series)

        # 6. Intensification vs extensification
        intensification = await self._compute_intensification(db, country)

        # Score computation
        score_components = []

        # Deforestation component (0-40)
        if deforestation_rate is not None:
            # > 1% annual deforestation is severe
            defor_score = max(0, min(40, deforestation_rate * 40.0))
            score_components.append(defor_score)
        else:
            score_components.append(20.0)  # neutral if unknown

        # Cropland expansion component (0-30)
        if cropland_expansion is not None:
            # Rapid expansion is unsustainable
            crop_score = max(0, min(30, abs(cropland_expansion) * 15.0))
            score_components.append(crop_score)
        else:
            score_components.append(15.0)

        # EKC position component (0-30)
        if ekc_result and ekc_result.get("turning_point"):
            tp = ekc_result["turning_point"]
            current_gdp = ekc_result.get("latest_gdp_per_capita", 0)
            if current_gdp > 0 and tp > 0:
                # Below turning point = still degrading
                if current_gdp < tp:
                    ekc_score = max(0, min(30, (1.0 - current_gdp / tp) * 30.0))
                else:
                    ekc_score = 0.0  # past turning point
                score_components.append(ekc_score)
            else:
                score_components.append(15.0)
        else:
            score_components.append(15.0)

        score = sum(score_components)

        return {
            "score": round(max(0.0, min(100.0, score)), 2),
            "country": country,
            "change_rates": change_rates,
            "deforestation_rate_annual_pct": round(deforestation_rate, 4) if deforestation_rate is not None else None,
            "cropland_expansion_annual_pct": round(cropland_expansion, 4) if cropland_expansion is not None else None,
            "transition_matrix": transition_matrix,
            "ekc_results": ekc_result,
            "intensification": intensification,
        }

    def _compute_transition_matrix(self, land_series: dict) -> dict | None:
        """Compute land use transition matrix from time series.

        Estimates transition probabilities between consecutive periods
        using relative share changes as a proxy for area transitions.
        """
        categories = [c for c in self.LAND_CATEGORIES if c in land_series]
        if len(categories) < 2:
            return None

        # Find common time periods
        all_dates = set()
        for data in land_series.values():
            all_dates.update(data["dates"])
        common_dates = sorted(all_dates)

        # Build area matrix: rows = time, cols = categories
        n_cats = len(categories)
        date_values = {}
        for cat in categories:
            d = land_series[cat]
            for dt, val in zip(d["dates"], d["values"]):
                if dt not in date_values:
                    date_values[dt] = {}
                date_values[dt][cat] = val

        # Find dates with all categories
        full_dates = [d for d in common_dates if all(c in date_values.get(d, {}) for c in categories)]
        if len(full_dates) < 2:
            return None

        # Compute transition proportions between consecutive periods
        trans = np.zeros((n_cats, n_cats), dtype=float)

        for t in range(len(full_dates) - 1):
            shares_t = np.array([date_values[full_dates[t]][c] for c in categories], dtype=float)
            shares_t1 = np.array([date_values[full_dates[t + 1]][c] for c in categories], dtype=float)

            total_t = shares_t.sum()
            total_t1 = shares_t1.sum()
            if total_t <= 0 or total_t1 <= 0:
                continue

            pct_t = shares_t / total_t
            pct_t1 = shares_t1 / total_t1

            # Approximate transition: proportional allocation of changes
            for i in range(n_cats):
                if pct_t[i] > 0:
                    for j in range(n_cats):
                        if i == j:
                            # Retention rate
                            trans[i, j] += min(pct_t1[j], pct_t[i])
                        else:
                            # Transfer proportional to gain in j
                            gain_j = max(0, pct_t1[j] - pct_t[j])
                            loss_i = max(0, pct_t[i] - pct_t1[i])
                            total_gain = sum(max(0, pct_t1[k] - pct_t[k]) for k in range(n_cats) if k != i)
                            if total_gain > 0 and loss_i > 0:
                                trans[i, j] += loss_i * (gain_j / total_gain)

        # Normalize rows
        row_sums = trans.sum(axis=1, keepdims=True)
        row_sums[row_sums == 0] = 1
        probs = trans / row_sums

        return {
            "categories": categories,
            "matrix": [[round(float(probs[i, j]), 4) for j in range(n_cats)] for i in range(n_cats)],
            "n_periods": len(full_dates) - 1,
        }

    async def _estimate_ekc(self, db, country: str, land_series: dict) -> dict | None:
        """Estimate Environmental Kuznets Curve for land use.

        D_t = b0 + b1*Y_t + b2*Y_t^2 + e_t
        where D = deforestation rate, Y = GDP per capita.
        Turning point: Y* = -b1 / (2*b2) if b2 < 0.
        """
        # Fetch GDP per capita time series
        gdp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.name LIKE '%GDP%per%capita%'
              AND ds.source IN ('wb', 'fred', 'imf')
            ORDER BY dp.date ASC
            """,
            (country,),
        )

        if not gdp_rows or "forest" not in land_series:
            return None

        gdp_by_date = {r["date"]: r["value"] for r in gdp_rows}
        forest_data = land_series["forest"]

        # Compute annual deforestation rates
        dates = []
        defor_rates = []
        gdp_vals = []

        for i in range(1, len(forest_data["values"])):
            dt = forest_data["dates"][i]
            if dt in gdp_by_date and forest_data["values"][i - 1] > 0:
                rate = -(forest_data["values"][i] - forest_data["values"][i - 1]) / forest_data["values"][i - 1] * 100
                dates.append(dt)
                defor_rates.append(rate)
                gdp_vals.append(gdp_by_date[dt])

        if len(dates) < 5:
            return None

        Y = np.array(gdp_vals, dtype=float)
        D = np.array(defor_rates, dtype=float)

        # Quadratic regression: D = b0 + b1*Y + b2*Y^2
        X = np.column_stack([np.ones(len(Y)), Y, Y ** 2])

        try:
            beta = np.linalg.lstsq(X, D, rcond=None)[0]
        except np.linalg.LinAlgError:
            return None

        b0, b1, b2 = beta
        resid = D - X @ beta
        ss_res = float(np.sum(resid ** 2))
        ss_tot = float(np.sum((D - np.mean(D)) ** 2))
        r_squared = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

        # Turning point
        turning_point = None
        if b2 < 0:
            turning_point = -b1 / (2 * b2)
            if turning_point < 0:
                turning_point = None

        return {
            "coefficients": {
                "constant": round(float(b0), 6),
                "gdp_per_capita": round(float(b1), 8),
                "gdp_per_capita_sq": round(float(b2), 12),
            },
            "r_squared": round(r_squared, 4),
            "turning_point": round(float(turning_point), 2) if turning_point is not None else None,
            "ekc_shape": "inverted_U" if b2 < 0 else "monotonic",
            "latest_gdp_per_capita": round(float(Y[-1]), 2),
            "n_obs": len(Y),
        }

    async def _compute_intensification(self, db, country: str) -> dict | None:
        """Decompose agricultural output growth into intensification vs extensification.

        Intensification = yield growth contribution
        Extensification = area growth contribution
        Total growth ~ area growth + yield growth (percentage decomposition)
        """
        # Fetch crop yield and area indices
        yield_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'fao'
              AND ds.name LIKE '%crop%yield%index%'
            ORDER BY dp.date ASC
            """,
            (country,),
        )
        area_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'fao'
              AND ds.name LIKE '%crop%area%index%'
            ORDER BY dp.date ASC
            """,
            (country,),
        )

        if len(yield_rows) < 3 or len(area_rows) < 3:
            return None

        yield_by_date = {r["date"]: r["value"] for r in yield_rows}
        area_by_date = {r["date"]: r["value"] for r in area_rows}
        common = sorted(set(yield_by_date) & set(area_by_date))

        if len(common) < 3:
            return None

        yields = np.array([yield_by_date[d] for d in common], dtype=float)
        areas = np.array([area_by_date[d] for d in common], dtype=float)

        # Production index = yield * area (approximately)
        production = yields * areas

        if production[0] <= 0 or yields[0] <= 0 or areas[0] <= 0:
            return None

        total_growth = (production[-1] / production[0] - 1.0) * 100.0
        yield_growth = (yields[-1] / yields[0] - 1.0) * 100.0
        area_growth = (areas[-1] / areas[0] - 1.0) * 100.0

        # Decomposition
        if abs(total_growth) > 0.01:
            intensification_share = yield_growth / total_growth * 100.0
            extensification_share = area_growth / total_growth * 100.0
        else:
            intensification_share = 50.0
            extensification_share = 50.0

        return {
            "total_production_growth_pct": round(total_growth, 2),
            "yield_growth_pct": round(yield_growth, 2),
            "area_growth_pct": round(area_growth, 2),
            "intensification_share_pct": round(intensification_share, 2),
            "extensification_share_pct": round(extensification_share, 2),
            "dominant_driver": "intensification" if yield_growth > area_growth else "extensification",
            "period": {"start": common[0], "end": common[-1]},
        }
