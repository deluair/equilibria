"""Subnational Accountability module.

Measures subnational accountability using control of corruption (CC.EST),
rule of law (RL.EST), and voice and accountability (VA.EST) from the World
Governance Indicators. Together these three dimensions capture whether
local government operates within a framework of legal oversight, citizen
voice, and anti-corruption norms.

Score reflects accountability deficit: high score = weak accountability.
Each WGI estimate: -2.5 to 2.5. Deficit = (0 - est) / 2.5 * 50 + 50.
Score = cc_stress * 0.4 + rl_stress * 0.35 + va_stress * 0.25.

Sources: WGI CC.EST, WGI RL.EST, WGI VA.EST.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class SubnationalAccountability(LayerBase):
    layer_id = "lLG"
    name = "Subnational Accountability"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        cc_code = "CC.EST"
        cc_name = "control of corruption"
        cc_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (cc_code, f"%{cc_name}%"),
        )

        rl_code = "RL.EST"
        rl_name = "rule of law"
        rl_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (rl_code, f"%{rl_name}%"),
        )

        va_code = "VA.EST"
        va_name = "voice and accountability"
        va_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (va_code, f"%{va_name}%"),
        )

        if not cc_rows and not rl_rows and not va_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no subnational accountability data"}

        cc_est = float(cc_rows[0]["value"]) if cc_rows else None
        rl_est = float(rl_rows[0]["value"]) if rl_rows else None
        va_est = float(va_rows[0]["value"]) if va_rows else None

        cc_stress = float(np.clip((0.0 - cc_est) / 2.5 * 50.0 + 50.0, 0, 100)) if cc_est is not None else 50.0
        rl_stress = float(np.clip((0.0 - rl_est) / 2.5 * 50.0 + 50.0, 0, 100)) if rl_est is not None else 50.0
        va_stress = float(np.clip((0.0 - va_est) / 2.5 * 50.0 + 50.0, 0, 100)) if va_est is not None else 50.0

        score = float(np.clip(cc_stress * 0.4 + rl_stress * 0.35 + va_stress * 0.25, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "control_of_corruption_est": round(cc_est, 3) if cc_est is not None else None,
            "rule_of_law_est": round(rl_est, 3) if rl_est is not None else None,
            "voice_accountability_est": round(va_est, 3) if va_est is not None else None,
            "corruption_stress_component": round(cc_stress, 1),
            "rule_of_law_stress_component": round(rl_stress, 1),
            "voice_stress_component": round(va_stress, 1),
            "interpretation": (
                "Severe accountability deficit: high corruption, weak rule of law"
                if score > 70
                else "Significant accountability weaknesses" if score > 50
                else "Moderate accountability gaps" if score > 30
                else "Strong subnational accountability framework"
            ),
            "_sources": ["WGI:CC.EST", "WGI:RL.EST", "WGI:VA.EST"],
        }
