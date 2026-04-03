"""Social Cohesion Resilience module.

Measures social cohesion as a disaster resilience factor. High inequality,
poor governance, and weak rule of law create fragile social fabric that
amplifies disaster impacts and slows collective recovery.

Indicators:
  SI.POV.GINI -- Gini index (0=perfect equality, 100=perfect inequality)
  GE.EST      -- Government effectiveness estimate (-2.5 to 2.5)
  RL.EST      -- Rule of law estimate (-2.5 to 2.5)

Score = clip(gini_penalty + governance_penalty + rule_penalty, 0, 100)
  gini_penalty: (gini - 25) * 0.8          (high inequality = fragile)
  governance_penalty: max(0, -ge) * 15     (poor governance = fragile)
  rule_penalty: max(0, -rl) * 15           (weak rule of law = fragile)

Sources: WDI (SI.POV.GINI, GE.EST, RL.EST)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class SocialCohesionResilience(LayerBase):
    layer_id = "lDE"
    name = "Social Cohesion Resilience"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        async def _fetch(series_id: str) -> list[float]:
            rows = await db.fetch_all(
                """
                SELECT dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                ORDER BY dp.date DESC
                LIMIT 10
                """,
                (country, series_id),
            )
            return [float(r["value"]) for r in rows if r["value"] is not None]

        gini_vals = await _fetch("SI.POV.GINI")
        ge_vals = await _fetch("GE.EST")
        rl_vals = await _fetch("RL.EST")

        if not gini_vals and not ge_vals and not rl_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        gini = float(np.mean(gini_vals)) if gini_vals else 35.0
        ge = float(np.mean(ge_vals)) if ge_vals else 0.0
        rl = float(np.mean(rl_vals)) if rl_vals else 0.0

        gini_penalty = float(np.clip((gini - 25.0) * 0.8, 0, 50))
        governance_penalty = float(np.clip(max(0.0, -ge) * 15.0, 0, 30))
        rule_penalty = float(np.clip(max(0.0, -rl) * 15.0, 0, 30))
        score = float(np.clip(gini_penalty + governance_penalty + rule_penalty, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "gini_index": round(gini, 4),
            "governance_effectiveness": round(ge, 4),
            "rule_of_law": round(rl, 4),
            "gini_penalty": round(gini_penalty, 2),
            "governance_penalty": round(governance_penalty, 2),
            "rule_penalty": round(rule_penalty, 2),
            "indicators": {
                "gini": "SI.POV.GINI",
                "governance": "GE.EST",
                "rule_of_law": "RL.EST",
            },
        }
