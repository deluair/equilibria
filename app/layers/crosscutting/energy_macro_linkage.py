"""Energy-Macro Linkage module.

Energy price pass-through to inflation (Hamilton 2009).

Uses CPI inflation (FP.CPI.TOTL.ZG) as the primary indicator.
When energy prices are volatile they transmit to general inflation
through input costs, transport, and expectations. High inflation
volatility (measured as standard deviation and coefficient of
variation) signals pass-through stress.

Score rises with inflation volatility and with sustained high
inflation levels.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class EnergyMacroLinkage(LayerBase):
    layer_id = "lCX"
    name = "Energy-Macro Linkage"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND (ds.series_id = 'FP.CPI.TOTL.ZG'
                   OR ds.series_id LIKE '%INFLATION%')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 6:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient inflation data",
            }

        vals = np.array([float(r["value"]) for r in rows if r["value"] is not None])
        dates = [r["date"] for r in rows if r["value"] is not None]

        if len(vals) < 6:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient non-null observations",
            }

        mean_inf = float(np.mean(vals))
        std_inf = float(np.std(vals, ddof=1))
        cv = std_inf / abs(mean_inf) if abs(mean_inf) > 1e-6 else std_inf

        # Recent volatility: last 5 observations vs full history
        recent = vals[-5:]
        recent_std = float(np.std(recent, ddof=1)) if len(recent) > 1 else std_inf
        volatility_ratio = recent_std / max(std_inf, 1e-6)

        # Score components
        # 1. Absolute level: >10% inflation -> stress
        level_score = float(np.clip(abs(mean_inf) / 10.0 * 40.0, 0.0, 40.0))

        # 2. Volatility (coefficient of variation): CV > 2 -> max stress
        vol_score = float(np.clip(cv / 2.0 * 40.0, 0.0, 40.0))

        # 3. Recent acceleration
        accel_score = float(np.clip((volatility_ratio - 1.0) * 10.0, 0.0, 20.0))

        score = min(100.0, level_score + vol_score + accel_score)

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(vals),
            "period": f"{dates[0]} to {dates[-1]}",
            "inflation_mean": round(mean_inf, 2),
            "inflation_std": round(std_inf, 2),
            "coefficient_of_variation": round(float(cv), 4),
            "recent_volatility_ratio": round(float(volatility_ratio), 4),
            "level_score": round(level_score, 2),
            "volatility_score": round(vol_score, 2),
            "acceleration_score": round(accel_score, 2),
            "interpretation": (
                "stable pass-through" if score < 25
                else "moderate pass-through" if score < 50
                else "high pass-through stress"
            ),
            "reference": "Hamilton 2009, JEL E32; Blanchard & Gali 2007",
        }
