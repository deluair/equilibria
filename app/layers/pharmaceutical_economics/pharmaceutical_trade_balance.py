"""Pharmaceutical trade balance: merchandise export receipts as proxy.

In the absence of pharmaceutical-specific trade data in the DB, total merchandise
export receipts (BX.GSR.MRCH.CD) proxy export capacity of the pharmaceutical
sector. Countries with strong export receipts relative to GDP tend to have
net-positive or near-balanced pharmaceutical trade positions.

Key references:
    Lakdawalla, D. et al. (2018). The economics of pharmaceutical pricing.
        Journal of Health Economics, 58, 1-20.
    Chaudhuri, S. (2005). The WTO and India's pharmaceuticals industry:
        patent protection, TRIPS, and developing countries. Oxford University Press.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PharmaceuticalTradeBalance(LayerBase):
    layer_id = "lPH"
    name = "Pharmaceutical Trade Balance"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Estimate pharmaceutical trade position from merchandise exports.

        Uses BX.GSR.MRCH.CD (merchandise export receipts, current USD) as a
        proxy for export capacity. Low export receipts relative to historical
        mean signal a weaker trade position and likely pharmaceutical import
        dependence.

        Returns dict with score, signal, and relevant metrics.
        """
        code = "BX.GSR.MRCH.CD"
        name = "merchandise exports"
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

        # Score: if latest exports are below historical mean -> rising import dependence.
        # Ratio = latest / mean. <1 = declining exports -> higher score (stress).
        ratio = latest / mean_val if mean_val > 0 else 1.0
        score = float(np.clip((1.0 - ratio) * 100 + 25, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "merch_exports_usd_latest": round(latest, 0),
                "merch_exports_usd_mean_15obs": round(mean_val, 0),
                "export_ratio_vs_mean": round(ratio, 3),
                "n_observations": len(values),
                "indicator": code,
            },
        }
