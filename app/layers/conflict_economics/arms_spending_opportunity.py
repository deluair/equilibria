"""Arms Spending Opportunity Cost module.

Measures the opportunity cost of military spending relative to social investment.
Uses military expenditure (MS.MIL.XPND.GD.ZS) compared to health and education
spending to quantify the tradeoff. High military spending crowding out social
spending signals high opportunity cost.

Score = clip(opportunity_cost_index * 100, 0, 100).
High score = severe opportunity cost (military crowds out social spending).

Sources: WDI (MS.MIL.XPND.GD.ZS, SH.XPD.CHEX.GD.ZS, SE.XPD.TOTL.GD.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ArmsSpendingOpportunity(LayerBase):
    layer_id = "lCW"
    name = "Arms Spending Opportunity Cost"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        mil_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'MS.MIL.XPND.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        health_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SH.XPD.CHEX.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        edu_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SE.XPD.TOTL.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not mil_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        mil_vals = [float(r["value"]) for r in mil_rows if r["value"] is not None]
        if not mil_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        mil_mean = float(np.mean(mil_vals))

        health_vals = [float(r["value"]) for r in health_rows if r["value"] is not None]
        edu_vals = [float(r["value"]) for r in edu_rows if r["value"] is not None]

        health_mean = float(np.mean(health_vals)) if health_vals else None
        edu_mean = float(np.mean(edu_vals)) if edu_vals else None

        social_mean = None
        if health_mean is not None and edu_mean is not None:
            social_mean = (health_mean + edu_mean) / 2
        elif health_mean is not None:
            social_mean = health_mean
        elif edu_mean is not None:
            social_mean = edu_mean

        # Military spending intensity (global avg ~2% GDP, conflict states often 4-10%)
        mil_intensity = float(np.clip((mil_mean / 4.0) * 50, 0, 60))

        # Crowding-out ratio: mil/(mil+social)
        if social_mean is not None and social_mean > 0:
            crowding_ratio = mil_mean / (mil_mean + social_mean)
            crowding_component = float(np.clip(crowding_ratio * 40, 0, 40))
        else:
            crowding_component = 0.0

        score = float(np.clip(mil_intensity + crowding_component, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "military_spending_pct_gdp": round(mil_mean, 4),
            "health_spending_pct_gdp": round(health_mean, 4) if health_mean is not None else None,
            "education_spending_pct_gdp": round(edu_mean, 4) if edu_mean is not None else None,
            "social_spending_mean_pct_gdp": round(social_mean, 4) if social_mean is not None else None,
            "crowding_ratio": round(mil_mean / (mil_mean + social_mean), 4) if social_mean is not None and social_mean > 0 else None,
            "mil_intensity_component": round(mil_intensity, 2),
            "crowding_component": round(crowding_component, 2),
            "n_obs": len(mil_vals),
            "indicators": {
                "military_spending": "MS.MIL.XPND.GD.ZS",
                "health_spending": "SH.XPD.CHEX.GD.ZS",
                "education_spending": "SE.XPD.TOTL.GD.ZS",
            },
        }
