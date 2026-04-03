"""Pharmaceutical spending share: health expenditure as % of GDP.

Health expenditure as a share of GDP captures aggregate healthcare resource
commitment, which underpins pharmaceutical market size and public medicine
financing capacity.

Key references:
    Xu, K. et al. (2010). Household catastrophic health expenditure: a
        multicountry analysis. The Lancet, 362(9378), 111-117.
    Dieleman, J. et al. (2016). Financing global health 2015. The Lancet,
        387(10037), 2521-2535.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PharmaceuticalSpendingShare(LayerBase):
    layer_id = "lPH"
    name = "Pharmaceutical Spending Share"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Estimate pharmaceutical sector size from health expenditure % GDP.

        Uses SH.XPD.CHEX.GD.ZS (current health expenditure as % of GDP).
        Very low shares signal underinvestment; very high shares may indicate
        inefficiency or high pharmaceutical burden.

        Returns dict with score, signal, and relevant metrics.
        """
        code = "SH.XPD.CHEX.GD.ZS"
        name = "health expenditure"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"No data for {code} in DB",
            }

        values = [float(row["value"]) for row in rows if row["value"] is not None]
        if not values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "All fetched rows have NULL value",
            }

        latest = values[0]
        mean_val = float(np.mean(values))

        # Score: underinvestment (<3% GDP) = high stress; optimal ~5-8%; >12% = high burden
        # Map: <3 -> 70+, 3-5 -> 50-70, 5-8 -> 20-40, 8-12 -> 40-60, >12 -> 70+
        if latest < 3.0:
            score = 70.0 + (3.0 - latest) * 10.0
        elif latest <= 5.0:
            score = 50.0 + (5.0 - latest) * 10.0
        elif latest <= 8.0:
            score = 20.0 + (latest - 5.0) * 6.67
        elif latest <= 12.0:
            score = 40.0 + (latest - 8.0) * 7.5
        else:
            score = 70.0 + (latest - 12.0) * 3.0

        score = float(np.clip(score, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "health_exp_pct_gdp_latest": round(latest, 2),
                "health_exp_pct_gdp_mean_15obs": round(mean_val, 2),
                "n_observations": len(values),
                "indicator": code,
            },
        }
