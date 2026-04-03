"""City Fiscal Capacity module.

Proxies municipal fiscal capacity using national tax revenue as a share of GDP.
Low national tax collection combined with high urbanization implies a city-level
fiscal gap: growing urban service demands outstrip revenue capacity.

Sources: WDI GC.TAX.TOTL.GD.ZS (tax revenue, % of GDP),
         WDI SP.URB.TOTL.IN.ZS (urban pop % of total).
Score = clip(urban_pct/100 * max(0, 20 - tax_gdp) * 5, 0, 100).
A country with 80% urban share and 5% tax/GDP scores 60: high fiscal gap.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

# Threshold below which national tax capacity is considered weak
_TAX_ADEQUACY_THRESHOLD = 20.0


class CityFiscalCapacity(LayerBase):
    layer_id = "lUE"
    name = "City Fiscal Capacity"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        tax_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.TAX.TOTL.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        urb_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SP.URB.TOTL.IN.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not tax_rows or not urb_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        tax_gdp = float(tax_rows[0]["value"])
        urban_pct = float(urb_rows[0]["value"])

        tax_gap = max(0.0, _TAX_ADEQUACY_THRESHOLD - tax_gdp)
        score = float(np.clip((urban_pct / 100.0) * tax_gap * 5.0, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "tax_revenue_pct_gdp": round(tax_gdp, 2),
            "urban_share_pct": round(urban_pct, 2),
            "tax_gap_from_threshold_ppt": round(tax_gap, 2),
            "fiscal_adequacy_threshold_pct_gdp": _TAX_ADEQUACY_THRESHOLD,
            "interpretation": (
                "Severe city fiscal gap: high urban demand, very low tax capacity"
                if score > 50
                else "Significant fiscal capacity shortfall" if score > 25
                else "Moderate fiscal-urban mismatch" if score > 10
                else "Adequate fiscal capacity relative to urbanization"
            ),
            "_sources": ["WDI:GC.TAX.TOTL.GD.ZS", "WDI:SP.URB.TOTL.IN.ZS"],
        }
