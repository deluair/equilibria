"""Trade Payment Risk module.

Contract enforcement risk for cross-border trade payments, combining
Rule of Law and Control of Corruption estimates. Weak institutions raise
counterparty payment risk, force exporters into costly insurance or
advance payment terms, and increase default rates on open-account trades.

Sources: WGI RL.EST (Rule of Law estimate, -2.5 to +2.5),
         WGI CC.EST (Control of Corruption estimate, -2.5 to +2.5)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class TradePaymentRisk(LayerBase):
    layer_id = "lTF"
    name = "Trade Payment Risk"

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
            return {"score": None, "signal": "UNAVAILABLE", "error": "no governance/contract enforcement data"}

        rl_val = float(rl_rows[0]["value"]) if rl_rows else 0.0
        cc_val = float(cc_rows[0]["value"]) if cc_rows else 0.0

        available_count = (1 if rl_rows else 0) + (1 if cc_rows else 0)
        composite = (rl_val + cc_val) / available_count  # -2.5 to +2.5

        # Map to 0-100 risk score: -2.5 => 100 (high risk), +2.5 => 0 (low risk)
        score = float(np.clip((2.5 - composite) / 5.0 * 100, 0, 100))

        return {
            "score": round(score, 2),
            "rule_of_law_est": round(rl_val, 3) if rl_rows else None,
            "control_of_corruption_est": round(cc_val, 3) if cc_rows else None,
            "governance_composite": round(composite, 3),
            "interpretation": "Lower governance scores signal higher counterparty payment risk in cross-border trade",
        }
