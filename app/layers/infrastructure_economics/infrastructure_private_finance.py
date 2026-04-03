"""Infrastructure Private Finance module.

Measures the health of the public-private partnership (PPP) pipeline for infrastructure.
Low private investment in infrastructure signals weak project pipeline, policy risk,
or fiscal constraints crowding out private finance.

Sources: WDI IE.PPI.TELE.CD (private investment in telecom, current USD),
         WDI IE.PPI.ENGY.CD (private investment in energy, current USD),
         WDI IE.PPI.TRAN.CD (private investment in transport, current USD),
         WDI NY.GDP.MKTP.CD (GDP current USD).
Score = clip(100 - (ppi_total / gdp * 1000), 0, 100).
PPI/GDP ratio scaled so 10% GDP of private infra investment -> score=0.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

PPI_SERIES = [
    ("IE.PPI.TELE.CD", "telecom"),
    ("IE.PPI.ENGY.CD", "energy"),
    ("IE.PPI.TRAN.CD", "transport"),
]


class InfrastructurePrivateFinance(LayerBase):
    layer_id = "lIF"
    name = "Infrastructure Private Finance"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        gdp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.MKTP.CD'
            ORDER BY dp.date DESC
            LIMIT 3
            """,
            (country,),
        )

        if not gdp_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no GDP data"}

        gdp = float(gdp_rows[0]["value"])
        if gdp <= 0:
            return {"score": None, "signal": "UNAVAILABLE", "error": "invalid GDP value"}

        ppi_components = {}
        ppi_total = 0.0
        for series_id, label in PPI_SERIES:
            rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                ORDER BY dp.date DESC
                LIMIT 3
                """,
                (country, series_id),
            )
            if rows:
                val = float(rows[0]["value"])
                ppi_components[label] = val
                ppi_total += val

        if not ppi_components:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no PPI data available"}

        ppi_pct_gdp = ppi_total / gdp * 100.0
        # High PPI/GDP -> low stress; benchmark 1% of GDP PPI = healthy pipeline
        score = float(np.clip(100.0 - ppi_pct_gdp * 100.0, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "ppi_total_usd": round(ppi_total, 0),
            "ppi_pct_gdp": round(ppi_pct_gdp, 4),
            "ppi_components_usd": {k: round(v, 0) for k, v in ppi_components.items()},
            "gdp_current_usd": round(gdp, 0),
            "interpretation": (
                "Very weak PPP pipeline: private finance largely absent from infrastructure"
                if score > 80
                else "Below-average private infrastructure finance" if score > 60
                else "Moderate private finance engagement" if score > 40
                else "Active private infrastructure investment pipeline"
            ),
            "_sources": ["WDI:IE.PPI.TELE.CD", "WDI:IE.PPI.ENGY.CD", "WDI:IE.PPI.TRAN.CD",
                         "WDI:NY.GDP.MKTP.CD"],
        }
