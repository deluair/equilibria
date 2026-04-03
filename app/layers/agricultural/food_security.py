"""Composite Food Security Index following FAO methodology.

Constructs a multi-dimensional food security index from four pillars defined
by the FAO (2013) "The State of Food Insecurity in the World":

1. **Availability**: domestic production, import capacity, food stocks.
   Indicators: average dietary energy supply adequacy, production value per
   capita, share of dietary energy supply from cereals/roots/tubers.

2. **Access**: income levels, food prices, market functioning.
   Indicators: GDP per capita (PPP), prevalence of undernourishment, food
   price level index, Gini coefficient.

3. **Utilization**: nutritional quality, sanitation, health.
   Indicators: stunting prevalence, wasting prevalence, access to improved
   water, dietary diversity score.

4. **Stability**: variability over time in availability and access.
   Indicators: cereal import dependency ratio, political stability index,
   per capita food supply variability, domestic food price volatility.

Each pillar is scored 0-100 (higher = more food insecure). The composite
index is a weighted average of pillar scores.

Score (0-100): Higher score indicates greater food insecurity.

References:
    FAO (2013). "The State of Food Insecurity in the World."
    FAO (2021). "Suite of Food Security Indicators." FAOSTAT.
"""

from __future__ import annotations

import numpy as np
from app.layers.base import LayerBase


class FoodSecurityIndex(LayerBase):
    layer_id = "l5"
    name = "Food Security Index"

    # Pillar weights (FAO-style equal weighting default)
    PILLAR_WEIGHTS = {
        "availability": 0.25,
        "access": 0.25,
        "utilization": 0.25,
        "stability": 0.25,
    }

    async def compute(self, db, **kwargs) -> dict:
        """Compute composite food security index.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code
            year : int - reference year
            pillar_weights : dict - optional custom weights
        """
        country = kwargs.get("country_iso3", "BGD")
        year = kwargs.get("year")
        weights = kwargs.get("pillar_weights", self.PILLAR_WEIGHTS)

        year_clause = "AND dp.date = ?" if year else ""

        pillar_scores = {}
        pillar_details = {}

        # --- Pillar 1: Availability ---
        avail_indicators = await self._fetch_indicators(
            db, country, year, year_clause,
            indicator_patterns=[
                ("dietary_energy_supply", "%dietary%energy%supply%adequacy%"),
                ("food_production_per_capita", "%food%production%per%capita%"),
                ("cereal_share_energy", "%cereal%share%dietary%energy%"),
            ],
        )
        avail_score, avail_detail = self._score_availability(avail_indicators)
        pillar_scores["availability"] = avail_score
        pillar_details["availability"] = avail_detail

        # --- Pillar 2: Access ---
        access_indicators = await self._fetch_indicators(
            db, country, year, year_clause,
            indicator_patterns=[
                ("gdp_per_capita_ppp", "%GDP%per%capita%PPP%"),
                ("undernourishment", "%prevalence%undernourishment%"),
                ("food_price_index", "%food%price%index%"),
                ("gini", "%gini%coefficient%"),
            ],
        )
        access_score, access_detail = self._score_access(access_indicators)
        pillar_scores["access"] = access_score
        pillar_details["access"] = access_detail

        # --- Pillar 3: Utilization ---
        util_indicators = await self._fetch_indicators(
            db, country, year, year_clause,
            indicator_patterns=[
                ("stunting", "%stunting%prevalence%"),
                ("wasting", "%wasting%prevalence%"),
                ("improved_water", "%improved%water%access%"),
                ("dietary_diversity", "%dietary%diversity%"),
            ],
        )
        util_score, util_detail = self._score_utilization(util_indicators)
        pillar_scores["utilization"] = util_score
        pillar_details["utilization"] = util_detail

        # --- Pillar 4: Stability ---
        stability_indicators = await self._fetch_stability(db, country, year)
        stab_score, stab_detail = self._score_stability(stability_indicators)
        pillar_scores["stability"] = stab_score
        pillar_details["stability"] = stab_detail

        # Composite score
        available_pillars = {k: v for k, v in pillar_scores.items() if v is not None}
        if not available_pillars:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no pillar data available",
            }

        # Re-normalize weights for available pillars
        total_weight = sum(weights[k] for k in available_pillars)
        composite = sum(
            pillar_scores[k] * weights[k] / total_weight for k in available_pillars
        )

        return {
            "score": round(composite, 2),
            "country": country,
            "year": year,
            "pillar_scores": {k: round(v, 2) if v is not None else None for k, v in pillar_scores.items()},
            "pillar_details": pillar_details,
            "n_pillars_available": len(available_pillars),
            "methodology": "FAO four-pillar framework",
        }

    async def _fetch_indicators(
        self, db, country: str, year: int | None, year_clause: str,
        indicator_patterns: list[tuple[str, str]],
    ) -> dict:
        """Fetch latest values for a list of indicator patterns."""
        results = {}
        for name, pattern in indicator_patterns:
            params = [country, pattern]
            if year:
                params.append(str(year))
            row = await db.fetch_one(
                f"""
                SELECT dp.value, dp.date
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.name LIKE ?
                  {year_clause}
                ORDER BY dp.date DESC
                LIMIT 1
                """,
                tuple(params),
            )
            if row:
                results[name] = {"value": row["value"], "date": row["date"]}
        return results

    async def _fetch_stability(self, db, country: str, year: int | None) -> dict:
        """Fetch stability-related indicators including variability measures."""
        results = {}

        # Cereal import dependency ratio
        row = await db.fetch_one(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.name LIKE '%cereal%import%dependency%'
            ORDER BY dp.date DESC LIMIT 1
            """,
            (country,),
        )
        if row:
            results["cereal_import_dependency"] = {"value": row["value"], "date": row["date"]}

        # Political stability
        row = await db.fetch_one(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.name LIKE '%political%stability%'
            ORDER BY dp.date DESC LIMIT 1
            """,
            (country,),
        )
        if row:
            results["political_stability"] = {"value": row["value"], "date": row["date"]}

        # Food supply variability: coefficient of variation over recent years
        supply_rows = await db.fetch_all(
            """
            SELECT dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.name LIKE '%food%supply%per%capita%'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )
        if len(supply_rows) >= 3:
            vals = np.array([r["value"] for r in supply_rows], dtype=float)
            cv = float(np.std(vals) / np.mean(vals)) if np.mean(vals) > 0 else 0.0
            results["supply_variability_cv"] = {"value": cv, "date": "recent"}

        # Domestic food price volatility
        price_rows = await db.fetch_all(
            """
            SELECT dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.name LIKE '%food%price%index%'
            ORDER BY dp.date DESC
            LIMIT 24
            """,
            (country,),
        )
        if len(price_rows) >= 6:
            vals = np.array([r["value"] for r in price_rows], dtype=float)
            # Month-over-month changes
            changes = np.diff(vals) / vals[:-1]
            volatility = float(np.std(changes))
            results["price_volatility"] = {"value": volatility, "date": "recent"}

        return results

    @staticmethod
    def _score_availability(indicators: dict) -> tuple[float | None, dict]:
        """Score availability pillar (0-100, higher = more insecure)."""
        scores = []
        detail = {}

        if "dietary_energy_supply" in indicators:
            des = indicators["dietary_energy_supply"]["value"]
            # DES adequacy: 100% = sufficient. Score: (130 - DES) normalized
            s = max(0, min(100, (130 - des) * 2.0)) if des < 130 else 0.0
            scores.append(s)
            detail["dietary_energy_supply"] = {"value": des, "sub_score": round(s, 2)}

        if "food_production_per_capita" in indicators:
            fpc = indicators["food_production_per_capita"]["value"]
            # Higher production = more secure. Invert and normalize
            # Using 200 as reference (index, 2004-2006=100)
            s = max(0, min(100, (200 - fpc) / 2.0)) if fpc < 200 else 0.0
            scores.append(s)
            detail["food_production_per_capita"] = {"value": fpc, "sub_score": round(s, 2)}

        if "cereal_share_energy" in indicators:
            cse = indicators["cereal_share_energy"]["value"]
            # High cereal dependence = less dietary diversity = more vulnerable
            s = max(0, min(100, cse))
            scores.append(s)
            detail["cereal_share_energy"] = {"value": cse, "sub_score": round(s, 2)}

        if not scores:
            return None, detail
        return float(np.mean(scores)), detail

    @staticmethod
    def _score_access(indicators: dict) -> tuple[float | None, dict]:
        """Score access pillar (0-100, higher = more insecure)."""
        scores = []
        detail = {}

        if "gdp_per_capita_ppp" in indicators:
            gdp = indicators["gdp_per_capita_ppp"]["value"]
            # Reference: $30,000 PPP = fully food secure
            s = max(0, min(100, (1.0 - gdp / 30000.0) * 80.0))
            scores.append(s)
            detail["gdp_per_capita_ppp"] = {"value": gdp, "sub_score": round(s, 2)}

        if "undernourishment" in indicators:
            und = indicators["undernourishment"]["value"]
            # Direct measure: % of population
            s = max(0, min(100, und * 2.0))  # 50% = score 100
            scores.append(s)
            detail["undernourishment"] = {"value": und, "sub_score": round(s, 2)}

        if "food_price_index" in indicators:
            fpi = indicators["food_price_index"]["value"]
            # Higher food prices relative to income = worse access
            s = max(0, min(100, (fpi - 80) * 1.5)) if fpi > 80 else 0.0
            scores.append(s)
            detail["food_price_index"] = {"value": fpi, "sub_score": round(s, 2)}

        if "gini" in indicators:
            gini = indicators["gini"]["value"]
            # Higher inequality = worse food access for poorest
            s = max(0, min(100, gini * 1.5))  # Gini 0-100 scale
            scores.append(s)
            detail["gini"] = {"value": gini, "sub_score": round(s, 2)}

        if not scores:
            return None, detail
        return float(np.mean(scores)), detail

    @staticmethod
    def _score_utilization(indicators: dict) -> tuple[float | None, dict]:
        """Score utilization pillar (0-100, higher = more insecure)."""
        scores = []
        detail = {}

        if "stunting" in indicators:
            st = indicators["stunting"]["value"]
            # Direct measure: % of children under 5
            s = max(0, min(100, st * 2.0))  # 50% = score 100
            scores.append(s)
            detail["stunting"] = {"value": st, "sub_score": round(s, 2)}

        if "wasting" in indicators:
            wa = indicators["wasting"]["value"]
            s = max(0, min(100, wa * 5.0))  # 20% = score 100
            scores.append(s)
            detail["wasting"] = {"value": wa, "sub_score": round(s, 2)}

        if "improved_water" in indicators:
            iw = indicators["improved_water"]["value"]
            # Higher = better (invert)
            s = max(0, min(100, 100.0 - iw))
            scores.append(s)
            detail["improved_water"] = {"value": iw, "sub_score": round(s, 2)}

        if "dietary_diversity" in indicators:
            dd = indicators["dietary_diversity"]["value"]
            # Higher diversity = better. Invert. Scale assumed 0-12 (HDDS)
            s = max(0, min(100, (12.0 - dd) / 12.0 * 100.0))
            scores.append(s)
            detail["dietary_diversity"] = {"value": dd, "sub_score": round(s, 2)}

        if not scores:
            return None, detail
        return float(np.mean(scores)), detail

    @staticmethod
    def _score_stability(indicators: dict) -> tuple[float | None, dict]:
        """Score stability pillar (0-100, higher = more insecure)."""
        scores = []
        detail = {}

        if "cereal_import_dependency" in indicators:
            cid = indicators["cereal_import_dependency"]["value"]
            # Higher dependency = less stable supply
            s = max(0, min(100, cid))
            scores.append(s)
            detail["cereal_import_dependency"] = {"value": cid, "sub_score": round(s, 2)}

        if "political_stability" in indicators:
            ps = indicators["political_stability"]["value"]
            # WGI scale: -2.5 (worst) to +2.5 (best). Invert and normalize.
            s = max(0, min(100, (2.5 - ps) / 5.0 * 100.0))
            scores.append(s)
            detail["political_stability"] = {"value": ps, "sub_score": round(s, 2)}

        if "supply_variability_cv" in indicators:
            cv = indicators["supply_variability_cv"]["value"]
            # CV > 0.10 is concerning; CV > 0.30 is crisis
            s = max(0, min(100, cv * 300.0))
            scores.append(s)
            detail["supply_variability_cv"] = {"value": cv, "sub_score": round(s, 2)}

        if "price_volatility" in indicators:
            vol = indicators["price_volatility"]["value"]
            # Monthly food price std dev > 0.05 is concerning
            s = max(0, min(100, vol * 1000.0))
            scores.append(s)
            detail["price_volatility"] = {"value": vol, "sub_score": round(s, 2)}

        if not scores:
            return None, detail
        return float(np.mean(scores)), detail
