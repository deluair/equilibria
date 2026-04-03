"""Regional Integration module.

Trade openness trend as a proxy for regional economic integration. A declining
trend in trade as a share of GDP signals de-integration, protectionism, or the
erosion of regional trade agreements (Viner 1950; Frankel 1997).

Trend estimated via OLS regression of trade openness on time index.
Score rises when the trend is negative (de-integration).

Sources: WDI (NE.TRD.GNFS.ZS trade + services openness)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class RegionalIntegration(LayerBase):
    layer_id = "lIN"
    name = "Regional Integration"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.TRD.GNFS.ZS'
            ORDER BY dp.date ASC
            LIMIT 30
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no trade openness data found",
            }

        valid = [(r["date"], float(r["value"])) for r in rows if r["value"] is not None]

        if len(valid) < 5:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient observations for trend estimation (need >= 5)",
            }

        dates, values = zip(*valid)
        x = np.arange(len(values), dtype=float)
        y = np.array(values, dtype=float)

        slope, intercept, r_value, p_value, std_err = linregress(x, y)

        latest_openness = values[-1]
        avg_openness = float(np.mean(y))

        # De-integration stress: negative slope -> higher score
        # Scale: slope of -1 pp/year over 10+ years = meaningful de-integration
        if slope < 0:
            # Magnitude of decline relative to average level
            relative_decline = abs(slope) / max(avg_openness, 1.0)
            score = float(np.clip(relative_decline * 500, 0, 100))
        else:
            score = 0.0

        return {
            "score": round(score, 1),
            "country": country,
            "trend_slope_pp_per_year": round(float(slope), 4),
            "trend_r_squared": round(float(r_value ** 2), 4),
            "trend_p_value": round(float(p_value), 4),
            "latest_openness_pct_gdp": round(latest_openness, 3),
            "avg_openness_pct_gdp": round(avg_openness, 3),
            "n_obs": len(valid),
            "de_integrating": slope < 0,
        }
