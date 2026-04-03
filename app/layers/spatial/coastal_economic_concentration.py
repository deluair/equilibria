"""Coastal economic concentration: maritime trade dependency risk.

Countries highly dependent on maritime trade for imports and exports face
concentration risk from port disruptions, sea lane vulnerability, and
coastal infrastructure bottlenecks. High merchandise import value and
high trade openness together signal coastal concentration.

Proxy: merchandise imports value (TM.VAL.MRCH.CD.WT) combined with
trade openness (NE.TRD.GNFS.ZS). High openness (>80%) + high import
dependence = elevated coastal concentration risk.

Score = clip((openness - 40) * 1.0, 0, 60) + import_concentration_bonus
import_concentration_bonus: if imports/GDP > 30% add 20; > 50% add 40.

References:
    Sachs, J.D. & Warner, A.M. (1995). Economic Reform and the Process of
        Global Integration. Brookings Papers on Economic Activity, 1995(1).
    Limao, N. & Venables, A.J. (2001). Infrastructure, Geographical Disadvantage.
        World Bank Economic Review, 15(3).

Sources: World Bank WDI TM.VAL.MRCH.CD.WT, NE.TRD.GNFS.ZS, NY.GDP.MKTP.CD.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CoastalEconomicConcentration(LayerBase):
    layer_id = "l11"
    name = "Coastal Economic Concentration"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        openness_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.TRD.GNFS.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        imports_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'TM.VAL.MRCH.CD.WT'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        gdp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.MKTP.CD'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not openness_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no trade openness data",
                "country": country,
            }

        openness = float(openness_rows[0]["value"])
        openness_year = openness_rows[0]["date"]

        # Base score from trade openness
        score = float(np.clip((openness - 40.0) * 1.0, 0.0, 60.0))

        imports_usd = None
        imports_to_gdp = None
        gdp_usd = None

        if imports_rows and gdp_rows:
            imports_usd = float(imports_rows[0]["value"])
            gdp_usd = float(gdp_rows[0]["value"])
            if gdp_usd > 0:
                imports_to_gdp = imports_usd / gdp_usd * 100.0
                if imports_to_gdp > 50.0:
                    score = min(100.0, score + 40.0)
                elif imports_to_gdp > 30.0:
                    score = min(100.0, score + 20.0)

        score = float(np.clip(score, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "country": country,
            "trade_openness_pct": round(openness, 2),
            "merchandise_imports_usd": round(imports_usd, 0) if imports_usd else None,
            "gdp_current_usd": round(gdp_usd, 0) if gdp_usd else None,
            "imports_to_gdp_pct": round(imports_to_gdp, 2) if imports_to_gdp else None,
            "year": openness_year,
            "dependency_level": (
                "high" if score > 60 else "moderate" if score > 30 else "low"
            ),
            "_source": "WDI NE.TRD.GNFS.ZS, TM.VAL.MRCH.CD.WT, NY.GDP.MKTP.CD",
        }
