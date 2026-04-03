"""Political Risk module.

Political instability and governance risk using WGI indicators:
  - PV.EST : Political Stability and Absence of Violence/Terrorism (z-score)
  - VA.EST  : Voice and Accountability (z-score)

Both indicators are z-scores in the range approximately [-2.5, 2.5].
Negative values indicate stress/weakness.

Score formula:
  score = clip(50 - (PV + VA) / 2 * 20, 0, 100)

This maps:
  - (PV + VA) = +2.5 + 2.5 = +5.0 -> score = 50 - 5.0 * 20 = 50 - 100 = -50 -> clipped to 0 (stable)
  - (PV + VA) = 0 -> score = 50 (moderate risk)
  - (PV + VA) = -2.5 + -2.5 = -5.0 -> score = 50 + 100 = 150 -> clipped to 100 (crisis)

Sources: World Bank WGI (Worldwide Governance Indicators).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PoliticalRisk(LayerBase):
    layer_id = "lRI"
    name = "Political Risk"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        async def fetch_mean(series_id: str, n: int = 5) -> float | None:
            rows = await db.fetch_all(
                """
                SELECT dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                  AND dp.value IS NOT NULL
                ORDER BY dp.date DESC
                LIMIT ?
                """,
                (country, series_id, n),
            )
            if not rows:
                return None
            return float(np.mean([float(r["value"]) for r in rows]))

        pv = await fetch_mean("PV.EST")
        va = await fetch_mean("VA.EST")

        if pv is None and va is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no political stability or voice/accountability data",
            }

        # Use 0 for missing component (neutral assumption)
        pv_val = pv if pv is not None else 0.0
        va_val = va if va is not None else 0.0

        avg = (pv_val + va_val) / 2.0
        score = float(np.clip(50.0 - avg * 20.0, 0, 100))

        flags = []
        if pv is not None and pv < -1.0:
            flags.append(f"political stability severely negative ({pv:.2f})")
        if va is not None and va < -1.0:
            flags.append(f"voice/accountability severely negative ({va:.2f})")

        return {
            "score": round(score, 1),
            "country": country,
            "indicators": {
                "PV.EST": round(pv, 4) if pv is not None else None,
                "VA.EST": round(va, 4) if va is not None else None,
            },
            "avg_wgi": round(avg, 4),
            "flags": flags,
            "interpretation": "WGI z-scores: >0 = better than median country, <0 = worse",
        }
