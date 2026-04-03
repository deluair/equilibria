"""Minimum wage bite: Kaitz index (minimum wage / median wage ratio).

The Kaitz index measures the relative generosity of the minimum wage. A high
ratio (bite) compresses the wage distribution from below, protecting low-wage
workers but potentially pricing some out of employment if set above the
competitive equilibrium (Card & Krueger 1994, Dube 2019).

A Kaitz index around 0.50-0.60 is typical of countries with effective minimum
wages. Below 0.35 the minimum wage has little bite. Above 0.75 disemployment
effects become more likely.

Scoring (stress at both extremes; minimum stress near 0.55):
    deviation = |kaitz - 0.55|
    score = clip(deviation * 200, 0, 100)

    kaitz = 0.55 -> score ~ 0   (moderate bite, low stress)
    kaitz = 0.35 -> score = 40  (weak bite)
    kaitz = 0.75 -> score = 40  (high bite, possible disemployment)
    kaitz = 1.0  -> score = 90  (extreme bite)

Sources: WDI / ILO (MW_MMET_SEX_ECO_NB_A — minimum-to-median wage ratio)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

SERIES = "MW_MMWM_NOC_RT"
OPTIMAL_BITE = 0.55


class MinimumWageBite(LayerBase):
    layer_id = "lLI"
    name = "Minimum Wage Bite"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'MW_MMWM_NOC_RT'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no minimum wage bite data (MW_MMWM_NOC_RT)",
            }

        latest_date = rows[0]["date"]
        kaitz = float(rows[0]["value"])

        deviation = abs(kaitz - OPTIMAL_BITE)
        score = float(np.clip(deviation * 200.0, 0.0, 100.0))

        if kaitz < 0.35:
            bite_level = "very low"
        elif kaitz < 0.50:
            bite_level = "low"
        elif kaitz <= 0.65:
            bite_level = "moderate"
        elif kaitz <= 0.75:
            bite_level = "high"
        else:
            bite_level = "very high"

        return {
            "score": round(score, 2),
            "country": country,
            "kaitz_index": round(kaitz, 4),
            "optimal_kaitz": OPTIMAL_BITE,
            "deviation_from_optimal": round(deviation, 4),
            "bite_level": bite_level,
            "latest_date": latest_date,
            "n_obs": len(rows),
            "note": (
                "score = clip(|kaitz - 0.55| * 200, 0, 100). "
                "Kaitz = min_wage / median_wage. Series: MW_MMWM_NOC_RT."
            ),
        }
