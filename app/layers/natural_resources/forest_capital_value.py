"""Forest capital value: forest wealth and deforestation rate.

Uses World Bank WDI:
  AG.LND.FRST.ZS    - forest area (% of land area)
  AG.LND.FRST.K2    - forest area (sq km)
  NY.ADJ.DFOR.GN.ZS - net forest depletion (% GNI)

Score combines deforestation pressure and depletion magnitude:
  s_depletion = clip(forest_depletion_pct_gni * 15, 0, 60)
  s_coverage  = clip((40 - forest_pct) / 40 * 40, 0, 40)  [low forest coverage adds risk]
  score = clip(s_depletion + s_coverage, 0, 100)

Sources: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ForestCapitalValue(LayerBase):
    layer_id = "lNR"
    name = "Forest Capital Value"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id IN (
                'AG.LND.FRST.ZS', 'AG.LND.FRST.K2', 'NY.ADJ.DFOR.GN.ZS'
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
                "error": "no forest data",
            }

        latest: dict[str, tuple[str, float]] = {}
        for r in rows:
            sid = r["series_id"]
            if sid not in latest and r["value"] is not None:
                latest[sid] = (r["date"][:4], float(r["value"]))

        forest_pct_data = latest.get("AG.LND.FRST.ZS")
        depletion_data = latest.get("NY.ADJ.DFOR.GN.ZS")

        if forest_pct_data is None and depletion_data is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no forest area or depletion data",
            }

        s_coverage = 0.0
        forest_pct = None
        forest_area_km2 = None
        if forest_pct_data:
            forest_pct = forest_pct_data[1]
            s_coverage = float(np.clip((40.0 - forest_pct) / 40.0 * 40.0, 0, 40))

        forest_km2_data = latest.get("AG.LND.FRST.K2")
        if forest_km2_data:
            forest_area_km2 = forest_km2_data[1]

        s_depletion = 0.0
        forest_depletion_pct = None
        if depletion_data:
            forest_depletion_pct = depletion_data[1]
            s_depletion = float(np.clip(forest_depletion_pct * 15.0, 0, 60))

        score = float(np.clip(s_depletion + s_coverage, 0, 100))

        forest_status = (
            "critical" if (forest_pct or 100) < 10
            else "depleted" if (forest_pct or 100) < 25
            else "moderate" if (forest_pct or 100) < 45
            else "adequate"
        )

        return {
            "score": round(score, 2),
            "results": {
                "country_iso3": country,
                "forest_area_pct_land": round(forest_pct, 3) if forest_pct is not None else None,
                "forest_area_km2": round(forest_area_km2, 0) if forest_area_km2 is not None else None,
                "forest_depletion_pct_gni": (
                    round(forest_depletion_pct, 4) if forest_depletion_pct is not None else None
                ),
                "sub_scores": {
                    "coverage_risk": round(s_coverage, 2),
                    "depletion_pressure": round(s_depletion, 2),
                },
                "forest_status": forest_status,
            },
        }
