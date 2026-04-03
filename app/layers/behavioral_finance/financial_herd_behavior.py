"""Financial Herd Behavior module.

Credit boom/bust pattern detection. Sudden large expansions followed by
contractions in private credit indicate herding: agents collectively
over-lend then abruptly reverse, amplifying cycles.

Sources: WDI FS.AST.PRVT.GD.ZS (domestic credit to private sector % of GDP)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class FinancialHerdBehavior(LayerBase):
    layer_id = "lBF"
    name = "Financial Herd Behavior"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            ("FS.AST.PRVT.GD.ZS", "%private sector%credit%"),
        )

        if not rows or len(rows) < 6:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        vals = np.array([float(r["value"]) for r in rows])
        vals = vals[::-1]  # chronological order

        changes = np.diff(vals)
        std_changes = float(np.std(changes))
        mean_abs_change = float(np.mean(np.abs(changes)))

        # Count sign reversals (boom -> bust transitions)
        sign_changes = int(np.sum(np.diff(np.sign(changes)) != 0))
        max_boom = float(np.max(changes))
        max_bust = float(np.min(changes))
        boom_bust_amplitude = max_boom - max_bust

        # High amplitude + frequent reversals = strong herding
        amplitude_score = np.clip(boom_bust_amplitude * 2, 0, 60)
        reversal_score = np.clip(sign_changes * 8, 0, 40)
        score = float(amplitude_score + reversal_score)

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(rows),
            "std_credit_changes": round(std_changes, 3),
            "mean_abs_change": round(mean_abs_change, 3),
            "sign_reversals": sign_changes,
            "max_credit_boom": round(max_boom, 3),
            "max_credit_bust": round(max_bust, 3),
            "boom_bust_amplitude": round(boom_bust_amplitude, 3),
            "interpretation": "Large boom-bust amplitude with frequent reversals indicates financial herding",
        }
