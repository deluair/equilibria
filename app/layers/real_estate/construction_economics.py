"""Construction Economics module.

Measures construction sector health via gross fixed capital formation trend
(NE.GDI.FTOT.ZS). A declining GFCF trend signals construction downturn and
reduced housing/infrastructure investment capacity.

Uses linregress on the trend to compute slope; negative slope = stress.
Score = clip(50 - slope * 5, 0, 100)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class ConstructionEconomics(LayerBase):
    layer_id = "lRE"
    name = "Construction Economics"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.GDI.FTOT.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 4:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient GFCF data for construction economics analysis",
            }

        vals = np.array([float(r["value"]) for r in rows])
        dates = [r["date"] for r in rows]
        t = np.arange(len(vals), dtype=float)

        slope, intercept, r_value, p_value, std_err = linregress(t, vals)

        latest_gfcf = float(vals[-1])
        avg_gfcf = float(np.mean(vals))

        # Negative slope = declining investment = higher stress
        # Normalize: slope of -2 ppts/yr -> score ~60; slope of +2 -> score ~40
        raw_score = 50 - float(slope) * 5
        score = float(np.clip(raw_score, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "gfcf_gdp_pct_latest": round(latest_gfcf, 2),
            "gfcf_gdp_pct_avg": round(avg_gfcf, 2),
            "trend_slope": round(float(slope), 4),
            "trend_r_squared": round(float(r_value ** 2), 4),
            "trend_p_value": round(float(p_value), 4),
            "period": f"{dates[0]} to {dates[-1]}",
            "n_obs": len(rows),
            "trend_direction": "DECLINING" if slope < 0 else "EXPANDING",
            "methodology": "linregress on GFCF/GDP; score = clip(50 - slope * 5, 0, 100)",
        }
