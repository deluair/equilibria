"""Health insurance coverage and prepayment analysis.

Estimates the prepayment share of health financing: total health expenditure
as % of GDP minus out-of-pocket share. A higher prepayment share signals
broader pooling mechanisms (insurance, taxation) and reduced catastrophic
spending risk.

Key references:
    Kutzin, J. (2013). Health financing for universal coverage and health
        system performance. Bulletin of the World Health Organization, 91, 602-611.
    World Bank WDI: SH.XPD.CHEX.GD.ZS, SH.XPD.OOPC.CH.ZS.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class HealthInsuranceCoverage(LayerBase):
    layer_id = "lHM"
    name = "Health Insurance Coverage"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Compute prepayment share as proxy for insurance coverage breadth.

        prepayment_share = 100 - OOP_share_of_CHE
        Higher prepayment -> better pooling -> lower score (less stress).
        """
        code_che = "SH.XPD.CHEX.GD.ZS"
        code_oop = "SH.XPD.OOPC.CH.ZS"

        che_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code_che, f"%{code_che}%"),
        )
        oop_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code_oop, f"%{code_oop}%"),
        )

        if not oop_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No OOP health expenditure data in DB",
            }

        oop_vals = [float(r["value"]) for r in oop_rows if r["value"] is not None]
        che_vals = [float(r["value"]) for r in che_rows if r["value"] is not None]

        if not oop_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No valid OOP values",
            }

        mean_oop = float(np.mean(oop_vals))
        prepayment_share = max(0.0, 100.0 - mean_oop)

        # Score: low prepayment = high stress
        # Full prepayment (100%) -> score 0; no prepayment (0%) -> score 100
        score = float(np.clip(100.0 - prepayment_share, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "mean_oop_pct_che": round(mean_oop, 2),
                "prepayment_share_pct": round(prepayment_share, 2),
                "mean_che_pct_gdp": round(float(np.mean(che_vals)), 2) if che_vals else None,
                "oop_n_obs": len(oop_vals),
                "che_n_obs": len(che_vals),
            },
        }
