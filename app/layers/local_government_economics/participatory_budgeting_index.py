"""Participatory Budgeting Index module.

Proxies participatory budgeting capacity using voice and accountability (VA.EST)
and government effectiveness (GE.EST) from the World Governance Indicators.
High VA.EST signals strong civic voice and public participation mechanisms;
high GE.EST signals administrative capacity to implement participatory outcomes.
Together they proxy the institutional prerequisites for participatory budgeting.

Score reflects deficit in participatory budgeting: high score = weak participation.
Score = clip(va_stress * 0.6 + ge_stress * 0.4, 0, 100).

Sources: WGI VA.EST, WGI GE.EST.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ParticipatoryBudgetingIndex(LayerBase):
    layer_id = "lLG"
    name = "Participatory Budgeting Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        va_code = "VA.EST"
        va_name = "voice and accountability"
        va_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (va_code, f"%{va_name}%"),
        )

        ge_code = "GE.EST"
        ge_name = "government effectiveness"
        ge_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (ge_code, f"%{ge_name}%"),
        )

        if not va_rows and not ge_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no participatory budgeting data"}

        va_est = float(va_rows[0]["value"]) if va_rows else None
        ge_est = float(ge_rows[0]["value"]) if ge_rows else None

        # WGI estimates range -2.5 to 2.5; low -> high stress
        va_stress = float(np.clip((0.0 - va_est) / 2.5 * 50.0 + 50.0, 0, 100)) if va_est is not None else 50.0
        ge_stress = float(np.clip((0.0 - ge_est) / 2.5 * 50.0 + 50.0, 0, 100)) if ge_est is not None else 50.0

        score = float(np.clip(va_stress * 0.6 + ge_stress * 0.4, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "voice_accountability_est": round(va_est, 3) if va_est is not None else None,
            "govt_effectiveness_est": round(ge_est, 3) if ge_est is not None else None,
            "voice_stress_component": round(va_stress, 1),
            "effectiveness_stress_component": round(ge_stress, 1),
            "interpretation": (
                "Very weak participatory budgeting environment"
                if score > 70
                else "Limited civic participation in budget processes" if score > 50
                else "Moderate participatory capacity" if score > 30
                else "Strong participatory budgeting prerequisites"
            ),
            "_sources": ["WGI:VA.EST", "WGI:GE.EST"],
        }
