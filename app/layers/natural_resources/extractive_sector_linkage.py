"""Extractive sector linkage: backward/forward linkages of extractive industries.

Estimates the degree to which extractive sectors (oil, gas, mining) generate
domestic economic linkages versus enclave effects. Uses:
  - NY.GDP.TOTL.RT.ZS  - total resource rents (% GDP) as extractive sector size proxy
  - NV.IND.TOTL.ZS     - industry value added (% GDP) [forward linkage proxy]
  - NV.MNF.TOTL.ZS.UN  - manufacturing value added (% GDP) [backward linkage proxy]

Linkage score (0-100, higher = weaker linkages / more enclave):
  Enclave index = rent_share / (manuf_pct + industry_pct + 1)
  score = clip(enclave_index * 5, 0, 100)

Strong linkages show high manufacturing/industry relative to rent share.

Sources: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ExtractiveSectorLinkage(LayerBase):
    layer_id = "lNR"
    name = "Extractive Sector Linkage"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id IN (
                'NY.GDP.TOTL.RT.ZS', 'NV.IND.TOTL.ZS', 'NV.MNF.TOTL.ZS.UN'
            )
              AND ds.country_iso3 = ?
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no extractive linkage indicator data",
            }

        latest: dict[str, float] = {}
        latest_year: dict[str, str] = {}
        for r in rows:
            sid = r["series_id"]
            if sid not in latest and r["value"] is not None:
                latest[sid] = float(r["value"])
                latest_year[sid] = r["date"][:4]

        rent_pct = latest.get("NY.GDP.TOTL.RT.ZS")
        if rent_pct is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "missing resource rent data",
            }

        industry_pct = latest.get("NV.IND.TOTL.ZS", 0.0)
        manuf_pct = latest.get("NV.MNF.TOTL.ZS.UN", 0.0)

        # Enclave index: high rents with low downstream production = high enclave
        enclave_index = rent_pct / (manuf_pct + industry_pct + 1.0)
        score = float(np.clip(enclave_index * 5.0, 0, 100))

        linkage_strength = (
            "strong" if score < 20
            else "moderate" if score < 45
            else "weak" if score < 70
            else "enclave"
        )

        return {
            "score": round(score, 2),
            "results": {
                "country_iso3": country,
                "rent_pct_gdp": round(rent_pct, 3),
                "industry_pct_gdp": round(industry_pct, 3),
                "manufacturing_pct_gdp": round(manuf_pct, 3),
                "enclave_index": round(enclave_index, 4),
                "linkage_strength": linkage_strength,
                "years": latest_year,
            },
        }
