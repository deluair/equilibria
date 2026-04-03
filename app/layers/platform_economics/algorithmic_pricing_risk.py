"""Algorithmic Pricing Risk module.

High consumer price inflation combined with weak control of corruption proxies the
risk that algorithmic pricing amplifies price instability and collusion.

Score: higher risk = higher score (worse).

Source: World Bank WDI (FP.CPI.TOTL.ZG), World Bank WGI (CC.EST inverted)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class AlgorithmicPricingRisk(LayerBase):
    layer_id = "lPE"
    name = "Algorithmic Pricing Risk"

    async def compute(self, db, **kwargs) -> dict:
        code = "FP.CPI.TOTL.ZG"
        name = "inflation consumer prices"
        cpi_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        code2 = "CC.EST"
        name2 = "control of corruption"
        cc_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (code2, f"%{name2}%"),
        )

        if not cpi_rows and not cc_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no CPI/corruption control data"}

        cpi_vals = [float(r["value"]) for r in cpi_rows if r["value"] is not None]
        cc_vals = [float(r["value"]) for r in cc_rows if r["value"] is not None]

        cpi_mean = float(np.nanmean(cpi_vals)) if cpi_vals else None
        cc_mean = float(np.nanmean(cc_vals)) if cc_vals else None

        components, weights = [], []
        if cpi_mean is not None:
            # CPI inflation %; cap at 50% as extreme, normalize to 0-100
            cpi_norm = float(np.clip(abs(cpi_mean) / 50.0 * 100, 0, 100))
            components.append(cpi_norm)
            weights.append(0.5)
        if cc_mean is not None:
            # CC.EST -2.5 to 2.5; invert so weak corruption control = high risk
            cc_inverted = float(np.clip((2.5 - cc_mean) / 5.0 * 100, 0, 100))
            components.append(cc_inverted)
            weights.append(0.5)

        if not components:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no usable values"}

        total_w = sum(weights)
        score = sum(c * w for c, w in zip(components, weights)) / total_w
        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 1),
            "cpi_inflation_pct": round(cpi_mean, 2) if cpi_mean is not None else None,
            "control_of_corruption_est": round(cc_mean, 3) if cc_mean is not None else None,
            "note": "CPI capped at 50%. CC.EST inverted: weaker control = higher algorithmic pricing risk.",
            "_citation": "World Bank WDI: FP.CPI.TOTL.ZG; World Bank WGI: CC.EST",
        }
