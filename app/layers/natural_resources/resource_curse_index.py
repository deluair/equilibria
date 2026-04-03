"""Resource curse index: composite Dutch disease / resource curse signal.

Combines four WDI indicators to detect symptoms of the resource curse:
  1. NY.GDP.TOTL.RT.ZS  - resource rents as % GDP (dependency)
  2. NV.MNF.TECH.ZS.UN  - medium/high-tech manufacturing (% MVA) [inverse]
  3. NE.EXP.GNFS.ZS     - exports of goods and services (% GDP) [concentration proxy]
  4. NY.GDP.MKTP.KD.ZG  - GDP growth (% annual) [growth drag]

Score = weighted combination of sub-scores (higher = more curse symptoms):
  - High rents + low manufacturing + growth underperformance -> high score

Sources: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_SERIES = [
    "NY.GDP.TOTL.RT.ZS",
    "NV.MNF.TECH.ZS.UN",
    "NE.EXP.GNFS.ZS",
    "NY.GDP.MKTP.KD.ZG",
]


class ResourceCurseIndex(LayerBase):
    layer_id = "lNR"
    name = "Resource Curse Index"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id IN (
                'NY.GDP.TOTL.RT.ZS', 'NV.MNF.TECH.ZS.UN',
                'NE.EXP.GNFS.ZS', 'NY.GDP.MKTP.KD.ZG'
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
                "error": "no resource curse indicator data",
            }

        # Collect most recent non-null value per series
        latest: dict[str, tuple[str, float]] = {}
        for r in rows:
            sid = r["series_id"]
            if sid not in latest and r["value"] is not None:
                latest[sid] = (r["date"][:4], float(r["value"]))

        rent_data = latest.get("NY.GDP.TOTL.RT.ZS")
        if rent_data is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "missing rent share data (core indicator)",
            }

        rent_pct = rent_data[1]
        # Sub-score 1: rent dependency (0-40)
        s_rent = float(np.clip(rent_pct * 1.6, 0, 40))

        # Sub-score 2: de-industrialisation (0-30) — low tech manuf = high score
        tech_data = latest.get("NV.MNF.TECH.ZS.UN")
        s_manuf = 0.0
        tech_pct = None
        if tech_data:
            tech_pct = tech_data[1]
            s_manuf = float(np.clip((50.0 - tech_pct) / 50.0 * 30.0, 0, 30))

        # Sub-score 3: export concentration (0-15) — very high exports can signal boom
        exp_data = latest.get("NE.EXP.GNFS.ZS")
        s_exp = 0.0
        exp_pct = None
        if exp_data:
            exp_pct = exp_data[1]
            # Commodity exporters tend to have export/GDP > 50%
            s_exp = float(np.clip((exp_pct - 30.0) / 70.0 * 15.0, 0, 15))

        # Sub-score 4: growth drag (0-15) — low or negative growth
        gdp_data = latest.get("NY.GDP.MKTP.KD.ZG")
        s_growth = 0.0
        gdp_growth = None
        if gdp_data:
            gdp_growth = gdp_data[1]
            s_growth = float(np.clip((5.0 - gdp_growth) / 10.0 * 15.0, 0, 15))

        score = float(np.clip(s_rent + s_manuf + s_exp + s_growth, 0, 100))

        return {
            "score": round(score, 2),
            "results": {
                "country_iso3": country,
                "rent_pct_gdp": round(rent_pct, 3),
                "tech_manufacturing_pct_mva": round(tech_pct, 3) if tech_pct is not None else None,
                "exports_pct_gdp": round(exp_pct, 3) if exp_pct is not None else None,
                "gdp_growth_pct": round(gdp_growth, 3) if gdp_growth is not None else None,
                "sub_scores": {
                    "rent_dependency": round(s_rent, 2),
                    "deindustrialisation": round(s_manuf, 2),
                    "export_concentration": round(s_exp, 2),
                    "growth_drag": round(s_growth, 2),
                },
                "indicators_available": len(latest),
            },
        }
