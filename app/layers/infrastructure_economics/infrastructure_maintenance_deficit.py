"""Infrastructure Maintenance Deficit module.

Estimates the gap in maintenance spending relative to the stock value.
Chronic under-maintenance leads to accelerated asset deterioration and
higher eventual replacement costs.

Sources: WDI NE.GDI.FTOT.ZS (GFCF % of GDP as investment proxy),
         WDI NY.GDP.MKTP.CD (GDP current USD) to derive absolute spending level.
Score = clip(max(0, maintenance_needed_pct - implied_maintenance_pct) * 20, 0, 100).
Benchmark: maintenance should be ~2-3% of infrastructure stock value (~1.5% of GDP).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

MAINTENANCE_NEEDED_PCT_GDP = 1.5  # % of GDP benchmark (World Bank estimate)
INFRA_SHARE_OF_GFCF = 0.30


class InfrastructureMaintenanceDeficit(LayerBase):
    layer_id = "lIF"
    name = "Infrastructure Maintenance Deficit"

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

        gfcf_pct = float(gfcf_rows[0]["value"])
        # Implied maintenance budget = fraction of infrastructure investment
        # Typically maintenance ~15-20% of new investment; use 15%
        implied_maintenance_pct = gfcf_pct * INFRA_SHARE_OF_GFCF * 0.15
        deficit = max(0.0, MAINTENANCE_NEEDED_PCT_GDP - implied_maintenance_pct)
        score = float(np.clip(deficit / MAINTENANCE_NEEDED_PCT_GDP * 100.0, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "gfcf_pct_gdp": round(gfcf_pct, 2),
            "implied_maintenance_pct_gdp": round(implied_maintenance_pct, 3),
            "maintenance_needed_pct_gdp": MAINTENANCE_NEEDED_PCT_GDP,
            "maintenance_deficit_ppt": round(deficit, 3),
            "reference_year": str(gfcf_rows[0]["date"]),
            "interpretation": (
                "Severe maintenance deficit: infrastructure stock deteriorating rapidly"
                if score > 60
                else "Significant maintenance shortfall" if score > 40
                else "Moderate maintenance gap" if score > 20
                else "Maintenance spending broadly adequate"
            ),
            "_sources": ["WDI:NE.GDI.FTOT.ZS"],
            "_note": "Proxy estimate; direct maintenance expenditure data rarely available.",
        }
