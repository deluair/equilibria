"""Post-Conflict Recovery module.

Measures the speed and depth of post-conflict economic recovery using GDP
growth trajectory. Looks for recovery acceleration (V-shaped rebound) or
stagnation after growth troughs. Uses NY.GDP.MKTP.KD.ZG to identify trough
years and subsequent recovery momentum.

Score = clip(recovery_index * 100, 0, 100).
High score = weak/slow recovery (worse outcome). Low score = strong recovery.

Sources: WDI (NY.GDP.MKTP.KD.ZG, NY.GDP.PCAP.KD)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PostConflictRecovery(LayerBase):
    layer_id = "lCW"
    name = "Post-Conflict Recovery"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        growth_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.MKTP.KD.ZG'
            ORDER BY dp.date
            LIMIT 30
            """,
            (country,),
        )

        gdppc_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.PCAP.KD'
            ORDER BY dp.date
            LIMIT 30
            """,
            (country,),
        )

        if not growth_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        growth_vals = [float(r["value"]) for r in growth_rows if r["value"] is not None]
        if len(growth_vals) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        arr = np.array(growth_vals)

        # Find trough (minimum growth)
        trough_idx = int(np.argmin(arr))
        trough_val = float(arr[trough_idx])

        # Post-trough recovery: average growth after trough
        post_trough = arr[trough_idx + 1:] if trough_idx < len(arr) - 1 else np.array([])
        post_trough_mean = float(np.mean(post_trough)) if len(post_trough) > 0 else 0.0

        # Recovery depth relative to trough severity
        recovery_gap = post_trough_mean - trough_val  # positive = recovery above trough

        # GDP per capita trajectory: persistent loss vs recovery
        gdppc_vals = [float(r["value"]) for r in gdppc_rows if r["value"] is not None]
        persistent_loss = 0.0
        if len(gdppc_vals) >= 5:
            gdppc_arr = np.array(gdppc_vals)
            first_half_mean = float(np.mean(gdppc_arr[: len(gdppc_arr) // 2]))
            second_half_mean = float(np.mean(gdppc_arr[len(gdppc_arr) // 2 :]))
            if first_half_mean > 0:
                persistent_loss = max((first_half_mean - second_half_mean) / first_half_mean, 0.0)

        # Score: high if recovery gap is small (weak recovery) or persistent GDP loss exists
        recovery_weakness = float(np.clip(max(5.0 - recovery_gap, 0) * 8, 0, 60))
        loss_component = float(np.clip(persistent_loss * 100, 0, 40))

        score = float(np.clip(recovery_weakness + loss_component, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "trough_growth_rate": round(trough_val, 4),
            "post_trough_growth_mean": round(post_trough_mean, 4),
            "recovery_gap": round(recovery_gap, 4),
            "persistent_gdppc_loss_ratio": round(persistent_loss, 4),
            "n_post_trough_obs": len(post_trough),
            "n_obs": len(growth_vals),
            "recovery_weakness_component": round(recovery_weakness, 2),
            "loss_component": round(loss_component, 2),
            "indicators": {
                "gdp_growth": "NY.GDP.MKTP.KD.ZG",
                "gdp_per_capita": "NY.GDP.PCAP.KD",
            },
        }
