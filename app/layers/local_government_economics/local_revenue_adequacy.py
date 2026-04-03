"""Local Revenue Adequacy module.

Proxies local revenue adequacy using national tax revenue as a share of GDP
(GC.TAX.TOTL.GD.ZS). Low national tax collection is a strong predictor of
inadequate subnational own-source revenue, as local tax bases mirror the
national capacity to collect. Property tax and local charges are even thinner
where the national system is weak.

Score reflects inadequacy: high score = high revenue stress.
Score = clip((25 - tax_gdp) / 25 * 100, 0, 100).

Sources: WDI GC.TAX.TOTL.GD.ZS.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_ADEQUATE_TAX_THRESHOLD = 25.0  # % of GDP considered adequate


class LocalRevenueAdequacy(LayerBase):
    layer_id = "lLG"
    name = "Local Revenue Adequacy"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        code = "GC.TAX.TOTL.GD.ZS"
        name = "tax revenue"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no tax revenue data"}

        values = [float(r["value"]) for r in rows if r["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid tax revenue values"}

        tax_gdp = values[0]
        avg_tax = float(np.mean(values))

        score = float(np.clip((_ADEQUATE_TAX_THRESHOLD - tax_gdp) / _ADEQUATE_TAX_THRESHOLD * 100.0, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "tax_revenue_pct_gdp": round(tax_gdp, 2),
            "avg_tax_pct_gdp_15yr": round(avg_tax, 2),
            "adequacy_threshold_pct_gdp": _ADEQUATE_TAX_THRESHOLD,
            "shortfall_ppt": round(max(0.0, _ADEQUATE_TAX_THRESHOLD - tax_gdp), 2),
            "interpretation": (
                "Critical local revenue shortfall"
                if score > 70
                else "Significant revenue inadequacy" if score > 50
                else "Moderate shortfall" if score > 25
                else "Revenue broadly adequate"
            ),
            "_sources": ["WDI:GC.TAX.TOTL.GD.ZS"],
        }
