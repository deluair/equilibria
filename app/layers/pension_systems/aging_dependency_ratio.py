"""Aging Dependency Ratio module.

Tracks old-age dependency ratio and its trend to measure fiscal stress on
pension systems. A rising ratio means fewer workers support each retiree.

Score = clip(ratio * 1.2, 0, 100). Ratio >= 58 triggers CRISIS signal.

Sources: WDI SP.POP.DPND.OL (old-age dependency ratio, per 100 working-age)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class AgingDependencyRatio(LayerBase):
    layer_id = "lPS"
    name = "Aging Dependency Ratio"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SP.POP.DPND.OL'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no dependency ratio data"}

        valid = [(r["date"], float(r["value"])) for r in rows if r["value"] is not None]
        if not valid:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid dependency data"}

        dates, values = zip(*valid)
        ratio = float(values[-1])
        score = float(np.clip(ratio * 1.2, 0, 100))

        trend_slope = None
        if len(values) >= 5:
            x = np.arange(len(values), dtype=float)
            slope, _, r_val, p_val, _ = linregress(x, np.array(values))
            trend_slope = round(float(slope), 4)

        return {
            "score": round(score, 1),
            "country": country,
            "latest_ratio": round(ratio, 2),
            "latest_date": dates[-1],
            "n_obs": len(values),
            "period": f"{dates[0]} to {dates[-1]}",
            "trend_slope_per_year": trend_slope,
            "rising": trend_slope > 0 if trend_slope is not None else None,
            "severe_aging_pressure": ratio >= 58.0,
            "interpretation": (
                "severe aging pressure" if ratio >= 58
                else "high aging pressure" if ratio >= 40
                else "moderate aging pressure" if ratio >= 25
                else "low aging pressure"
            ),
            "sources": ["WDI SP.POP.DPND.OL"],
        }
