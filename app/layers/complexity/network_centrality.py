"""Network Centrality module.

Trade network centrality proxy: trade openness level and trend.
High and growing openness = good network position (core).
Low or declining openness = periphery stress.

Score = 100 - clipped(openness * trend_factor, 0, 100)

Sources: WDI NE.TRD.GNFS.ZS (trade % GDP)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class NetworkCentrality(LayerBase):
    layer_id = "lCP"
    name = "Network Centrality"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.TRD.GNFS.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        values = np.array([float(r["value"]) for r in rows])
        dates = [r["date"] for r in rows]

        latest_openness = float(values[-1])

        # Trend: OLS slope over last 10 years (or all available)
        window = values[-10:] if len(values) >= 10 else values
        x = np.arange(len(window), dtype=float)
        slope = float(np.polyfit(x, window, 1)[0])

        # High openness (>100% GDP for small open economies is normal) -> low stress
        # Normalize openness: cap at 200 for scoring
        openness_norm = min(latest_openness / 200.0, 1.0)

        # Positive slope = growing integration = reduces stress
        trend_factor = 1.0 - min(max(slope / 5.0, -0.3), 0.3)

        # Periphery stress = 100 - network position
        network_position = openness_norm * 100.0 * (1.0 - max(trend_factor - 1.0, 0.0))
        score = float(max(0.0, min(100.0, 100.0 - network_position)))

        return {
            "score": round(score, 1),
            "country": country,
            "trade_openness_pct_gdp": round(latest_openness, 2),
            "openness_trend_slope_per_year": round(slope, 3),
            "period": f"{dates[0]} to {dates[-1]}",
            "n_obs": len(values),
            "interpretation": (
                "High score = periphery stress (low/declining openness). "
                "Low score = core network position (high/growing trade integration)."
            ),
            "_citation": "World Bank WDI: NE.TRD.GNFS.ZS",
        }
