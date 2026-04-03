"""Creative Industries module.

Manufacturing exports as share of merchandise exports is used as a
proxy for creative and industrial value-added capacity. Countries with
low manufacturing export shares rely heavily on commodity or primary
exports, indicating limited creative/industrial capacity.

Queries TX.VAL.MANF.ZS.UN (Manufactures exports, % of merchandise exports).

Benchmark: > 60% is considered strong industrial capacity.
Scoring: score = clip((60 - manf_share) / 60 * 100, 0, 100)
- manf_share = 60% -> score = 0 (no stress)
- manf_share = 30% -> score = 50
- manf_share = 0%  -> score = 100

Sources: WDI (TX.VAL.MANF.ZS.UN)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

MANF_SERIES = "TX.VAL.MANF.ZS.UN"
BENCHMARK_PCT = 60.0


class CreativeIndustries(LayerBase):
    layer_id = "lCU"
    name = "Creative Industries"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'TX.VAL.MANF.ZS.UN'
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows or len(rows) < 5:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data (need >= 5 rows)",
            }

        values = np.array([float(r["value"]) for r in rows], dtype=float)
        latest_val = float(values[0])
        mean_val = float(np.mean(values))

        score = float(np.clip((BENCHMARK_PCT - latest_val) / BENCHMARK_PCT * 100.0, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(rows),
            "manf_export_share_latest_pct": round(latest_val, 4),
            "manf_export_share_mean_pct": round(mean_val, 4),
            "benchmark_pct": BENCHMARK_PCT,
            "period": f"{rows[-1]['date']} to {rows[0]['date']}",
            "note": "score = clip((60 - manf_share) / 60 * 100, 0, 100); benchmark = 60% of merchandise exports",
        }
