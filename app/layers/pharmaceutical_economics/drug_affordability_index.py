"""Drug affordability index: price level relative to income.

Uses the PPP conversion factor (private consumption, PA.NUS.PRVT.PP) as a
proxy for price levels relative to international benchmarks. A high PPP factor
relative to income indicates that drug prices are burdensome for households.

Key references:
    Cameron, A. et al. (2009). Medicine prices, availability, and affordability
        in 36 developing and middle-income countries. The Lancet, 373, 240-249.
    Niëns, L.M. et al. (2010). Quantifying the impoverishing effects of
        purchasing medicines: a cross-country comparison of the affordability
        of medicines in the developing world. PLOS Medicine, 7(8).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class DrugAffordabilityIndex(LayerBase):
    layer_id = "lPH"
    name = "Drug Affordability Index"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Construct drug affordability proxy from PPP conversion factor.

        Uses PA.NUS.PRVT.PP (PPP conversion factor, private consumption).
        Higher values relative to income indicate less affordable price levels.
        Fetches recent observations and derives an affordability stress score.

        Returns dict with score, signal, and relevant metrics.
        """
        code = "PA.NUS.PRVT.PP"
        name = "PPP conversion factor"
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

        # PPP factor: lower = prices competitive vs USD; higher = more expensive locally
        # Normalize: assume median global PPP ~50 LCU/USD; >100 = high price burden
        # Score rises with PPP (price burden)
        score = float(np.clip((latest / 150.0) * 100, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "ppp_factor_latest": round(latest, 4),
                "ppp_factor_mean_15obs": round(mean_val, 4),
                "n_observations": len(values),
                "indicator": code,
            },
        }
