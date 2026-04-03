"""Institutional Trust Index module.

Composite of rule of law (RL.EST) and government effectiveness (GE.EST),
both from World Bank WGI. Higher values indicate better institutional quality.

Score formula:
  composite = mean(RL.EST, GE.EST) on [-2.5, +2.5] scale
  score = clip(50 - composite * 20, 0, 100)
  High score = low institutional trust (stress signal)

Sources: World Bank WDI (RL.EST, GE.EST)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_INDICATORS = ["RL.EST", "GE.EST"]


class InstitutionalTrustIndex(LayerBase):
    layer_id = "lSC"
    name = "Institutional Trust Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT ds.series_id, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('RL.EST', 'GE.EST')
            ORDER BY ds.series_id, dp.date
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for RL.EST or GE.EST",
            }

        latest: dict[str, float] = {}
        series_values: dict[str, list[float]] = {}
        for r in rows:
            series_values.setdefault(r["series_id"], []).append(float(r["value"]))
        for sid, vals in series_values.items():
            latest[sid] = vals[-1]

        if not latest:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        composite = float(np.mean(list(latest.values())))
        score = float(np.clip(50.0 - composite * 20.0, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "composite_wgi": round(composite, 4),
            "rule_of_law": round(latest.get("RL.EST", float("nan")), 4),
            "govt_effectiveness": round(latest.get("GE.EST", float("nan")), 4),
            "n_indicators": len(latest),
            "note": "WGI scale: -2.5 (worst) to +2.5 (best); high score = low trust",
        }
