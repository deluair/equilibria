"""Systemic fragility index module.

Composite of NPL ratio (FB.AST.NPER.ZS) and private credit depth (FS.AST.PRVT.GD.ZS).
High NPLs combined with deep credit markets amplify systemic risk non-linearly.

Score (0-100): interaction of asset quality deterioration and credit depth.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

NPL_CODE = "FB.AST.NPER.ZS"
NPL_NAME = "bank nonperforming loans"
CREDIT_CODE = "FS.AST.PRVT.GD.ZS"
CREDIT_NAME = "domestic credit private sector"


class SystemicFragilityIndex(LayerBase):
    layer_id = "lFC"
    name = "Systemic Fragility Index"

    async def compute(self, db, **kwargs) -> dict:
        npl_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (NPL_CODE, f"%{NPL_NAME}%"),
        )
        credit_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (CREDIT_CODE, f"%{CREDIT_NAME}%"),
        )

        if not npl_rows and not credit_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no systemic fragility data"}

        npl_vals = [float(r["value"]) for r in npl_rows if r["value"] is not None]
        credit_vals = [float(r["value"]) for r in credit_rows if r["value"] is not None]

        npl_latest = npl_vals[0] if npl_vals else None
        credit_latest = credit_vals[0] if credit_vals else None

        # NPL component: nonlinear -- doubling NPLs more than doubles risk
        npl_score = 30.0
        if npl_latest is not None:
            npl_score = float(np.clip(npl_latest ** 1.3 * 5.0, 0, 100))

        # Credit depth amplifier: deep credit markets mean NPL shocks propagate further
        credit_amplifier = 1.0
        if credit_latest is not None:
            credit_amplifier = 1.0 + float(np.clip((credit_latest - 30.0) / 100.0, 0, 1.0))

        # Interaction term: NPL x credit depth
        interaction_score = 0.0
        if npl_latest is not None and credit_latest is not None:
            interaction_score = float(np.clip(npl_latest * (credit_latest / 50.0) * 4.0, 0, 60))

        raw_score = 0.40 * npl_score * credit_amplifier + 0.60 * interaction_score
        score = float(np.clip(raw_score, 0, 100))

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "npl_ratio_pct": round(npl_latest, 2) if npl_latest is not None else None,
                "private_credit_gdp_pct": round(credit_latest, 2) if credit_latest is not None else None,
                "credit_amplifier": round(credit_amplifier, 3),
                "npl_component": round(npl_score, 2),
                "interaction_score": round(interaction_score, 2),
            },
        }
