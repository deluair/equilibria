"""Long-Run Growth module.

Fits a linear trend to GDP per capita (constant USD) and scores based on
the direction and strength of the trend. A strongly negative trend or low
explanatory power indicates structural growth failure.

Indicator: NY.GDP.PCAP.KD (GDP per capita, constant 2015 USD, WDI).
Method: scipy.stats.linregress over all available years.
Score: 100 = strongly negative trend (crisis), 0 = strongly positive trend (stable).
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class LongRunGrowth(LayerBase):
    layer_id = "lHI"
    name = "Long-Run Growth"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.PCAP.KD'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        years = np.array([float(r["date"][:4]) for r in rows])
        values = np.array([float(r["value"]) for r in rows])

        slope, intercept, r_value, p_value, std_err = linregress(years, values)
        r_squared = r_value ** 2

        # Normalise slope by mean level to get an annualised % change proxy
        mean_level = float(np.mean(values))
        if mean_level == 0:
            return {"score": None, "signal": "UNAVAILABLE", "error": "zero mean GDP per capita"}

        slope_pct = slope / mean_level  # fractional annual change

        # Score: negative slope_pct -> high stress score.
        # Map slope_pct in [-0.05, +0.05] linearly: -0.05 -> 100, +0.05 -> 0.
        raw = (-slope_pct / 0.05) * 50 + 50
        score = float(np.clip(raw, 0, 100))

        # Low R2 with negative slope amplifies uncertainty (partial adjustment).
        if slope < 0 and r_squared < 0.5:
            score = float(np.clip(score * (1 + (0.5 - r_squared)), 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(rows),
            "period": f"{rows[0]['date'][:4]} to {rows[-1]['date'][:4]}",
            "slope": round(float(slope), 4),
            "slope_pct_annual": round(slope_pct * 100, 4),
            "r_squared": round(r_squared, 4),
            "p_value": round(float(p_value), 4),
            "mean_gdp_pc": round(mean_level, 2),
            "latest_gdp_pc": round(float(values[-1]), 2),
        }
