"""Medicine supply chain: logistics performance as supply chain quality proxy.

Pharmaceutical supply chains depend critically on logistics infrastructure for
cold chain management, timely distribution, and last-mile delivery. The World
Bank Logistics Performance Index (LPI) overall score is a validated proxy.

Key references:
    Arvis, J.F. et al. (2018). Connecting to Compete 2018: Trade Logistics
        in the Global Economy. World Bank.
    Frost, L.J. & Reich, M.R. (2008). Access: How do good health technologies
        get to poor people in poor countries? Harvard Center for Population.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MedicineSupplyChain(LayerBase):
    layer_id = "lPH"
    name = "Medicine Supply Chain"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Assess medicine supply chain quality via logistics performance index.

        Uses LP.LPI.OVRL.XQ (Logistics Performance Index, overall, 1-5 scale).
        Higher LPI = better logistics = stronger pharmaceutical supply chain.
        Score is inverted: poor logistics -> high stress score.

        Returns dict with score, signal, and relevant metrics.
        """
        code = "LP.LPI.OVRL.XQ"
        name = "logistics performance index"
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

        # LPI range: 1 (worst) to 5 (best). Invert: score = (5 - lpi) / 4 * 100
        score = float(np.clip(((5.0 - latest) / 4.0) * 100, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "lpi_overall_latest": round(latest, 3),
                "lpi_overall_mean_15obs": round(mean_val, 3),
                "n_observations": len(values),
                "indicator": code,
                "scale": "1 (worst) to 5 (best)",
            },
        }
