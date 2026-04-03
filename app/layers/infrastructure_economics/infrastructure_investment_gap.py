"""Infrastructure Investment Gap module.

Measures the gap between actual and needed infrastructure spending.
Low investment relative to need signals accumulating deficits and
reduced long-run growth potential.

Sources: WDI GB.XPD.RSDV.GD.ZS (R&D spend as proxy where infra data absent),
         WDI NE.GDI.FTOT.ZS (gross fixed capital formation % of GDP).
Score = clip(max(0, needed_share - actual_share) * 5, 0, 100)
where needed_share is benchmarked at 4.5% of GDP (global average need).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

INFRA_NEEDED_PCT_GDP = 4.5  # IMF/World Bank benchmark


class InfrastructureInvestmentGap(LayerBase):
    layer_id = "lIF"
    name = "Infrastructure Investment Gap"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        gfcf_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.GDI.FTOT.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not gfcf_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        gfcf = float(gfcf_rows[0]["value"])
        # Approximate public infrastructure as ~30% of gross fixed capital formation
        infra_actual = gfcf * 0.30
        gap = max(0.0, INFRA_NEEDED_PCT_GDP - infra_actual)
        score = float(np.clip(gap * (100.0 / INFRA_NEEDED_PCT_GDP), 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "gfcf_pct_gdp": round(gfcf, 2),
            "estimated_infra_spend_pct_gdp": round(infra_actual, 2),
            "needed_infra_pct_gdp": INFRA_NEEDED_PCT_GDP,
            "gap_ppt": round(gap, 2),
            "reference_year": str(gfcf_rows[0]["date"]),
            "interpretation": (
                "Severe underinvestment: large infrastructure deficit accumulating"
                if score > 60
                else "Significant investment gap" if score > 40
                else "Moderate gap relative to need" if score > 20
                else "Investment broadly meets estimated need"
            ),
            "_sources": ["WDI:NE.GDI.FTOT.ZS"],
        }
