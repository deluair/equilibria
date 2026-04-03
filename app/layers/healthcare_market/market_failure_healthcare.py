"""Market failure in healthcare analysis.

Information asymmetry is a core source of healthcare market failure (Arrow, 1963).
Proxied here using OOP share (patients paying without insurer oversight) and
account ownership (financial inclusion as a proxy for access to formal risk-
pooling mechanisms).

Key references:
    Arrow, K.J. (1963). Uncertainty and the welfare economics of medical care.
        American Economic Review, 53(5), 941-973.
    World Bank WDI: SH.XPD.OOPC.CH.ZS, FX.OWN.TOTL.ZS.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MarketFailureHealthcare(LayerBase):
    layer_id = "lHM"
    name = "Market Failure Healthcare"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Estimate market failure severity from OOP and financial inclusion.

        High OOP + low financial inclusion -> severe information asymmetry
        and market failure. Score increases with failure severity.
        """
        code_oop = "SH.XPD.OOPC.CH.ZS"
        code_fi = "FX.OWN.TOTL.ZS"

        oop_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code_oop, f"%{code_oop}%"),
        )
        fi_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code_fi, f"%{code_fi}%"),
        )

        if not oop_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No OOP health expenditure data in DB",
            }

        oop_vals = [float(r["value"]) for r in oop_rows if r["value"] is not None]
        fi_vals = [float(r["value"]) for r in fi_rows if r["value"] is not None]

        if not oop_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No valid OOP values",
            }

        mean_oop = float(np.mean(oop_vals))
        # OOP component: 0-100 scale
        oop_score = float(np.clip(mean_oop, 0, 100))

        if fi_vals:
            mean_fi = float(np.mean(fi_vals))
            # Low financial inclusion amplifies market failure
            fi_penalty = float(np.clip(100.0 - mean_fi, 0, 100)) * 0.3
        else:
            fi_penalty = 15.0  # conservative default penalty
            mean_fi = None

        score = float(np.clip(oop_score * 0.7 + fi_penalty, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "mean_oop_pct_che": round(mean_oop, 2),
                "mean_account_ownership_pct": round(mean_fi, 2) if mean_fi is not None else None,
                "oop_n_obs": len(oop_vals),
                "fi_n_obs": len(fi_vals),
                "market_failure_driver": "information_asymmetry_proxy",
            },
        }
