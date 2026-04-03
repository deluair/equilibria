"""Local Government Capacity module.

Measures local government administrative and regulatory capacity using
government effectiveness (GE.EST) and regulatory quality (RQ.EST) from
the World Governance Indicators. Both reflect the competence of public
institutions to formulate and implement policies, closely mirroring
subnational administrative capacity.

Score reflects capacity deficit: high score = weak local capacity.
GE.EST and RQ.EST range roughly -2.5 to 2.5; normalized so 0 = median stress.

Score = clip((0 - ge_est)/2.5 * 50 + 50) * 0.5 + clip((0 - rq_est)/2.5 * 50 + 50) * 0.5

Sources: WGI GE.EST, WGI RQ.EST.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class LocalGovernmentCapacity(LayerBase):
    layer_id = "lLG"
    name = "Local Government Capacity"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        ge_code = "GE.EST"
        ge_name = "government effectiveness"
        ge_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (ge_code, f"%{ge_name}%"),
        )

        rq_code = "RQ.EST"
        rq_name = "regulatory quality"
        rq_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (rq_code, f"%{rq_name}%"),
        )

        if not ge_rows and not rq_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no governance capacity data"}

        ge_est = float(ge_rows[0]["value"]) if ge_rows else None
        rq_est = float(rq_rows[0]["value"]) if rq_rows else None

        ge_stress = float(np.clip((0.0 - ge_est) / 2.5 * 50.0 + 50.0, 0, 100)) if ge_est is not None else 50.0
        rq_stress = float(np.clip((0.0 - rq_est) / 2.5 * 50.0 + 50.0, 0, 100)) if rq_est is not None else 50.0

        score = float(np.clip(ge_stress * 0.5 + rq_stress * 0.5, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "govt_effectiveness_est": round(ge_est, 3) if ge_est is not None else None,
            "regulatory_quality_est": round(rq_est, 3) if rq_est is not None else None,
            "ge_stress_component": round(ge_stress, 1),
            "rq_stress_component": round(rq_stress, 1),
            "interpretation": (
                "Very weak local government capacity: high institutional failure risk"
                if score > 70
                else "Weak capacity: subnational governance significantly hampered" if score > 50
                else "Moderate capacity gaps" if score > 30
                else "Adequate local government capacity"
            ),
            "_sources": ["WGI:GE.EST", "WGI:RQ.EST"],
        }
