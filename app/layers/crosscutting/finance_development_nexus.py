"""Finance-Development Nexus module.

King & Levine (1993) finance-growth nexus.

Domestic credit to private sector (% GDP) as financial depth proxy
vs GDP per capita growth. Financial deepening should precede and
support growth. High credit expansion without corresponding growth
signals misallocation, over-leverage, or credit bubble risk.

Score rises when credit is high relative to trend AND growth is low
(stress = finance without real-economy payoff).
"""

from __future__ import annotations

import numpy as np
from scipy.stats import pearsonr

from app.layers.base import LayerBase


class FinanceDevelopmentNexus(LayerBase):
    layer_id = "lCX"
    name = "Finance-Development Nexus"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows_credit = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id LIKE 'FS.AST.DOMS.GD.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        rows_growth = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.MKTP.KD.ZG'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows_credit or not rows_growth:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data for domestic credit or GDP growth",
            }

        credit_map = {r["date"]: float(r["value"]) for r in rows_credit if r["value"] is not None}
        growth_map = {r["date"]: float(r["value"]) for r in rows_growth if r["value"] is not None}

        common_dates = sorted(set(credit_map) & set(growth_map))
        if len(common_dates) < 8:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"only {len(common_dates)} overlapping observations (need 8+)",
            }

        credit_vals = np.array([credit_map[d] for d in common_dates])
        growth_vals = np.array([growth_map[d] for d in common_dates])

        corr, p_value = pearsonr(credit_vals, growth_vals)

        # Stress = high credit + low growth -> negative or weak correlation
        # Also penalise extremely high credit-to-GDP (over-leverage risk)
        credit_mean = float(np.mean(credit_vals))
        growth_mean = float(np.mean(growth_vals))

        # Base score from negative correlation
        corr_stress = float(np.clip((1.0 - corr) / 2.0 * 60.0, 0.0, 60.0))

        # Additional penalty for high credit depth (>100% GDP) with low growth (<2%)
        leverage_penalty = 0.0
        if credit_mean > 100.0 and growth_mean < 2.0:
            leverage_penalty = min(40.0, (credit_mean - 100.0) / 5.0)

        score = min(100.0, corr_stress + leverage_penalty)

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(common_dates),
            "period": f"{common_dates[0]} to {common_dates[-1]}",
            "credit_gdp_mean": round(credit_mean, 2),
            "gdp_growth_mean": round(growth_mean, 2),
            "correlation": round(float(corr), 4),
            "p_value": round(float(p_value), 4),
            "leverage_penalty": round(leverage_penalty, 2),
            "interpretation": (
                "finance supports growth" if corr > 0.3 and growth_mean > 2.0
                else "over-leveraged" if credit_mean > 100.0 and growth_mean < 2.0
                else "weak nexus"
            ),
            "reference": "King & Levine 1993, QJE 108(3)",
        }
