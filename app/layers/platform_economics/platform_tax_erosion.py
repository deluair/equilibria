"""Platform Tax Erosion module.

High computer/communications services exports combined with low tax-to-GDP ratio
signals risk of digital platform tax base erosion.

Score: higher erosion risk = higher score (worse).

Source: World Bank WDI (BX.GSR.CMCP.ZS, GC.TAX.TOTL.GD.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PlatformTaxErosion(LayerBase):
    layer_id = "lPE"
    name = "Platform Tax Erosion"

    async def compute(self, db, **kwargs) -> dict:
        code = "BX.GSR.CMCP.ZS"
        name = "computer communications services exports"
        svc_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        code2 = "GC.TAX.TOTL.GD.ZS"
        name2 = "tax revenue"
        tax_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (code2, f"%{name2}%"),
        )

        if not svc_rows and not tax_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no services trade/tax revenue data"}

        svc_vals = [float(r["value"]) for r in svc_rows if r["value"] is not None]
        tax_vals = [float(r["value"]) for r in tax_rows if r["value"] is not None]

        svc_mean = float(np.nanmean(svc_vals)) if svc_vals else None
        tax_mean = float(np.nanmean(tax_vals)) if tax_vals else None

        components, weights = [], []
        if svc_mean is not None:
            # Higher services export share suggests more platform activity (risk driver)
            components.append(float(np.clip(svc_mean, 0, 100)))
            weights.append(0.5)
        if tax_mean is not None:
            # Lower tax revenue = higher erosion risk; invert (cap at 40% GDP)
            tax_inverted = float(np.clip((40.0 - tax_mean) / 40.0 * 100, 0, 100))
            components.append(tax_inverted)
            weights.append(0.5)

        if not components:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no usable values"}

        total_w = sum(weights)
        score = sum(c * w for c, w in zip(components, weights)) / total_w
        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 1),
            "comms_services_exports_pct": round(svc_mean, 2) if svc_mean is not None else None,
            "tax_revenue_pct_gdp": round(tax_mean, 2) if tax_mean is not None else None,
            "note": "Tax revenue inverted: lower collection = higher erosion risk.",
            "_citation": "World Bank WDI: BX.GSR.CMCP.ZS, GC.TAX.TOTL.GD.ZS",
        }
