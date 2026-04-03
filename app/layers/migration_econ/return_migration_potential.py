"""Return Migration Potential module.

Assesses conditions for diaspora return migration by examining trends
in government effectiveness and GDP per capita growth. Improving
governance and rising incomes create positive pull conditions.

When conditions improve, the return migration score should decrease
(lower stress = better for development). Stagnant or deteriorating
conditions produce high scores, indicating low return potential.

Score is inverted: improving conditions = lower stress score.
Declining or stagnant conditions = higher stress.

Sources: WDI (GE.EST trend, NY.GDP.PCAP.KD.ZG)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ReturnMigrationPotential(LayerBase):
    layer_id = "lME"
    name = "Return Migration Potential"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        gov_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GE.EST'
            ORDER BY dp.date ASC
            LIMIT 10
            """,
            (country,),
        )

        growth_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.PCAP.KD.ZG'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not gov_rows and not growth_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        gov_vals = [float(r["value"]) for r in gov_rows if r["value"] is not None]
        growth_vals = [float(r["value"]) for r in growth_rows if r["value"] is not None]

        # Governance trend: positive slope = improving conditions
        if len(gov_vals) >= 3:
            x = np.arange(len(gov_vals), dtype=float)
            gov_trend = float(np.polyfit(x, gov_vals, 1)[0])
        else:
            gov_trend = 0.0

        gov_level = float(gov_vals[-1]) if gov_vals else 0.0
        gdp_growth = float(np.mean(growth_vals)) if growth_vals else 2.0

        # Governance improvement: positive trend reduces stress
        # Governance level: higher = better; negative = stress
        gov_level_stress = float(np.clip(-gov_level * 15, 0, 45))
        gov_trend_stress = float(np.clip(-gov_trend * 100, 0, 25))

        # GDP growth: low/negative growth reduces return incentive
        growth_stress = float(np.clip(max(0.0, 3.0 - gdp_growth) * 10, 0, 30))

        score = gov_level_stress + gov_trend_stress + growth_stress

        return {
            "score": round(score, 1),
            "country": country,
            "gov_effectiveness_latest": round(gov_level, 4),
            "gov_effectiveness_trend": round(gov_trend, 5),
            "gdp_per_capita_growth_pct": round(gdp_growth, 2),
            "n_gov_obs": len(gov_vals),
            "components": {
                "governance_level_stress": round(gov_level_stress, 2),
                "governance_trend_stress": round(gov_trend_stress, 2),
                "low_growth_stress": round(growth_stress, 2),
            },
            "interpretation": (
                "very low return potential" if score > 65
                else "limited return potential" if score > 40
                else "improving conditions for return"
            ),
        }
