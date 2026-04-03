"""Correspondent Banking Risk module.

De-risking pressure on correspondent banking relationships, proxied by
inverted institutional quality indicators. Weak rule of law and high
perceived corruption lead global correspondent banks to terminate or
restrict relationships, cutting off the domestic banking system from
international trade finance networks (SWIFT, LC confirmation, etc.).

Sources: WGI RL.EST (Rule of Law estimate),
         WGI CC.EST (Control of Corruption estimate)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CorrespondentBankingRisk(LayerBase):
    layer_id = "lTF"
    name = "Correspondent Banking Risk"

    async def compute(self, db, **kwargs) -> dict:
        rl_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            ("RL.EST", "%rule%law%estimate%"),
        )
        cc_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            ("CC.EST", "%control%corruption%estimate%"),
        )

        if not rl_rows and not cc_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no institutional quality data"}

        rl_val = float(rl_rows[0]["value"]) if rl_rows else 0.0
        cc_val = float(cc_rows[0]["value"]) if cc_rows else 0.0

        available_count = (1 if rl_rows else 0) + (1 if cc_rows else 0)
        composite = (rl_val + cc_val) / available_count  # -2.5 to +2.5

        # De-risking pressure is highest at low institutional quality
        # Weighted more heavily toward corruption (AML/CFT driver of de-risking)
        if rl_rows and cc_rows:
            weighted = 0.4 * rl_val + 0.6 * cc_val
        else:
            weighted = composite

        # Map -2.5..+2.5 => 100..0 (inverted: bad institutions = high risk)
        score = float(np.clip((2.5 - weighted) / 5.0 * 100, 0, 100))

        return {
            "score": round(score, 2),
            "rule_of_law_est": round(rl_val, 3) if rl_rows else None,
            "control_of_corruption_est": round(cc_val, 3) if cc_rows else None,
            "derisking_pressure_index": round(score, 2),
            "interpretation": "Weak institutions and high corruption raise AML/CFT risk, driving correspondent bank de-risking",
        }
