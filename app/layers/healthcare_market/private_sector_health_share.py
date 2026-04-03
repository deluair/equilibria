"""Private sector health expenditure share analysis.

Measures the private sector's role in health financing. An excessively high
private share may indicate under-investment in public health systems and
greater inequality in access. An optimal mixed system has balanced public-
private financing.

Key references:
    Preker, A.S. & Harding, A. (2003). Innovations in Health Service Delivery:
        The Corporatization of Public Hospitals. World Bank.
    World Bank WDI: SH.XPD.PVTD.CH.ZS.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PrivateSectorHealthShare(LayerBase):
    layer_id = "lHM"
    name = "Private Sector Health Share"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Score private health expenditure share.

        Very high private share (>70%) indicates public system underfunding.
        Very low (<10%) may reflect suppressed private markets.
        Moderate range (30-60%) is considered balanced.
        """
        code = "SH.XPD.PVTD.CH.ZS"

        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{code}%"),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No private health expenditure data in DB",
            }

        vals = [float(r["value"]) for r in rows if r["value"] is not None]
        if not vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No valid private health expenditure values",
            }

        mean_pvt = float(np.mean(vals))
        # Stress peaks at extremes: >70% or <10%
        if mean_pvt > 70.0:
            stress = (mean_pvt - 70.0) / 30.0
        elif mean_pvt < 10.0:
            stress = (10.0 - mean_pvt) / 10.0 * 0.5
        elif mean_pvt > 60.0:
            stress = (mean_pvt - 60.0) / 10.0 * 0.5
        else:
            stress = 0.0

        score = float(np.clip(stress * 100, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "mean_private_health_share_pct": round(mean_pvt, 2),
                "high_stress_threshold": 70.0,
                "low_stress_threshold": 10.0,
                "n_obs": len(vals),
            },
        }
