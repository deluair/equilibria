"""Multilateral Negotiations module.

Tariff regime openness as a proxy for the health of multilateral trade negotiations.
Rising mean tariff rates signal breakdown in WTO/regional negotiation processes and
protectionist retrenchment (Bagwell & Staiger 2002; WTO 2023). Declining tariffs
indicate successful negotiation and trade liberalization.

Score = clip(latest_tariff * 5, 0, 100).
A 20% average tariff maps to score 100 (maximum negotiation breakdown stress).

Sources: WDI (TM.TAX.MRCH.WM.AR.ZS weighted mean tariff rate, all products)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class MultilateralNegotiations(LayerBase):
    layer_id = "lIN"
    name = "Multilateral Negotiations"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'TM.TAX.MRCH.WM.AR.ZS'
            ORDER BY dp.date DESC
            LIMIT 15
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no tariff rate data found",
            }

        valid = [(r["date"], float(r["value"])) for r in rows if r["value"] is not None]

        if not valid:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no valid tariff observations",
            }

        dates, values = zip(*valid)
        latest_tariff = values[0]  # rows ordered DESC so first is latest
        avg_tariff = float(np.mean(values))

        # Primary score: level of latest tariff rate
        score = float(np.clip(latest_tariff * 5, 0, 100))

        # Trend: rising tariff -> adjustment for accelerating protectionism
        trend_slope = None
        trend_direction = "insufficient data"
        if len(values) >= 4:
            x = np.arange(len(values), dtype=float)
            y = np.array(values[::-1], dtype=float)  # chronological order
            slope, _, r_val, p_val, _ = linregress(x, y)
            trend_slope = float(slope)
            if slope > 0.1:
                trend_direction = "rising"
                # Amplify score by up to 20 points for rising tariffs
                score = float(np.clip(score + slope * 10, 0, 100))
            elif slope < -0.1:
                trend_direction = "falling"
            else:
                trend_direction = "stable"

        return {
            "score": round(score, 1),
            "country": country,
            "latest_tariff_rate_pct": round(latest_tariff, 3),
            "avg_tariff_rate_pct": round(avg_tariff, 3),
            "n_obs": len(valid),
            "trend_slope_pp_per_year": round(trend_slope, 4) if trend_slope is not None else None,
            "tariff_trend": trend_direction,
            "high_protection": latest_tariff > 10,
        }
