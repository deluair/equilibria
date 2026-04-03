"""Healthcare price regulation analysis.

Combines out-of-pocket burden with regulatory quality as a proxy for
the effectiveness of healthcare price regulation. High OOP under strong
regulatory capacity signals inadequate price controls; high OOP under
weak regulation indicates systemic market failure.

Key references:
    Papanicolas, I., Woskie, L.R. & Jha, A.K. (2018). Health care spending
        in the United States and other high-income countries. JAMA, 319(10).
    World Bank WDI: SH.XPD.OOPC.CH.ZS; Worldwide Governance Indicators: RQ.EST.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class HealthcarePriceRegulation(LayerBase):
    layer_id = "lHM"
    name = "Healthcare Price Regulation"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Score price regulation adequacy from OOP burden and regulatory quality.

        High OOP + low regulatory quality -> high stress score.
        """
        code_oop = "SH.XPD.OOPC.CH.ZS"
        code_rq = "RQ.EST"

        oop_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code_oop, f"%{code_oop}%"),
        )
        rq_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code_rq, f"%{code_rq}%"),
        )

        if not oop_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No OOP health expenditure data in DB",
            }

        oop_vals = [float(r["value"]) for r in oop_rows if r["value"] is not None]
        rq_vals = [float(r["value"]) for r in rq_rows if r["value"] is not None]

        if not oop_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No valid OOP values",
            }

        mean_oop = float(np.mean(oop_vals))
        # OOP stress: >40% = high, 20-40% = moderate, <20% = low
        oop_stress = float(np.clip(mean_oop / 60.0, 0, 1))

        # Regulatory quality: WGI RQ.EST ranges roughly -2.5 to +2.5
        if rq_vals:
            mean_rq = float(np.mean(rq_vals))
            # Normalize to [0,1] where 1 = strong regulation
            rq_norm = float(np.clip((mean_rq + 2.5) / 5.0, 0, 1))
            # Strong regulation should reduce OOP stress
            effective_stress = oop_stress * (1.0 - 0.3 * rq_norm)
        else:
            effective_stress = oop_stress
            rq_norm = None

        score = float(np.clip(effective_stress * 100, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "mean_oop_pct_che": round(mean_oop, 2),
                "mean_regulatory_quality": (
                    round(float(np.mean(rq_vals)), 3) if rq_vals else None
                ),
                "rq_normalized": round(rq_norm, 3) if rq_norm is not None else None,
                "oop_n_obs": len(oop_vals),
                "rq_n_obs": len(rq_vals),
            },
        }
