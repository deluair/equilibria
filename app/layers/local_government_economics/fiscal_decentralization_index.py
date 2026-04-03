"""Fiscal Decentralization Index module.

Proxies fiscal decentralization using national tax revenue (GC.TAX.TOTL.GD.ZS)
as a share of GDP combined with government effectiveness (GE.EST). Countries with
low tax-to-GDP ratios alongside weak government effectiveness signal poor subnational
revenue capacity and inadequate fiscal devolution.

Score reflects fiscal centralization stress: high score = high centralization risk.
Score = clip((100 - norm_tax) * 0.6 + norm_ge_deficit * 0.4, 0, 100)

Sources: WDI GC.TAX.TOTL.GD.ZS, WGI GE.EST.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class FiscalDecentralizationIndex(LayerBase):
    layer_id = "lLG"
    name = "Fiscal Decentralization Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        code = "GC.TAX.TOTL.GD.ZS"
        name = "tax revenue"
        tax_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        ge_code = "GE.EST"
        ge_name = "government effectiveness"
        ge_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (ge_code, f"%{ge_name}%"),
        )

        if not tax_rows and not ge_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no fiscal decentralization data"}

        tax_gdp = None
        if tax_rows:
            tax_gdp = float(tax_rows[0]["value"])

        ge_est = None
        if ge_rows:
            ge_est = float(ge_rows[0]["value"])

        # Normalize tax: 0-100 scale (15% -> 80, 30% -> 0 stress)
        if tax_gdp is not None:
            tax_stress = float(np.clip((30.0 - tax_gdp) / 30.0 * 100.0, 0, 100))
        else:
            tax_stress = 50.0

        # GE.EST ranges roughly -2.5 to 2.5; low GE -> high stress
        if ge_est is not None:
            ge_stress = float(np.clip((0.0 - ge_est) / 2.5 * 50.0 + 50.0, 0, 100))
        else:
            ge_stress = 50.0

        score = float(np.clip(tax_stress * 0.6 + ge_stress * 0.4, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "tax_revenue_pct_gdp": round(tax_gdp, 2) if tax_gdp is not None else None,
            "govt_effectiveness_est": round(ge_est, 3) if ge_est is not None else None,
            "tax_stress_component": round(tax_stress, 1),
            "ge_stress_component": round(ge_stress, 1),
            "interpretation": (
                "Severe fiscal centralization: very low revenue capacity"
                if score > 70
                else "High centralization risk" if score > 50
                else "Moderate decentralization stress" if score > 30
                else "Adequate fiscal decentralization"
            ),
            "_sources": ["WDI:GC.TAX.TOTL.GD.ZS", "WGI:GE.EST"],
        }
