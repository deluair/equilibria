"""Biodiversity Pressure: forest cover loss and agricultural land expansion.

Measures the combined pressure on biodiversity from deforestation and agricultural
expansion. Declining forest area share combined with rising agricultural land share
signals intensifying biodiversity pressure. Both trends are weighted equally.

Methodology:
    Fit linear trends:
        forest_slope = d(forest_%)/dt  (negative = deforestation)
        agri_slope   = d(agri_%)/dt    (positive = expansion)

    Deforestation pressure  = clip(-forest_slope * 20, 0, 50)
    Agriculture expansion   = clip( agri_slope   * 20, 0, 50)
    score = deforestation_pressure + agriculture_expansion

    Additionally, level pressure:
        if forest_pct < 10 -> add 10 to score (extreme deforestation)

References:
    Maxwell, S. et al. (2016). "Biodiversity: The ravages of guns, nets and
        bulldozers." Nature, 536(7615), 143-145.
    IPBES (2019). Global Assessment Report on Biodiversity and Ecosystem Services.
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class BiodiversityPressure(LayerBase):
    layer_id = "lSU"
    name = "Biodiversity Pressure"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('AG.LND.FRST.ZS', 'AG.LND.AGRI.ZS')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 6:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient forest/agricultural land data"}

        series: dict[str, dict[str, float]] = {}
        for r in rows:
            sid = r["series_id"]
            yr = r["date"][:4] if isinstance(r["date"], str) else str(r["date"])
            series.setdefault(sid, {})[yr] = float(r["value"])

        forest = series.get("AG.LND.FRST.ZS", {})
        agri = series.get("AG.LND.AGRI.ZS", {})

        common = sorted(set(forest.keys()) & set(agri.keys()))
        if len(common) < 4:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient matched forest/agri data"}

        years = np.array([int(y) for y in common])
        forest_arr = np.array([forest[y] for y in common])
        agri_arr = np.array([agri[y] for y in common])
        t = years - years[0]

        forest_slope, _, _, _, _ = stats.linregress(t, forest_arr)
        agri_slope, _, _, _, _ = stats.linregress(t, agri_arr)

        deforestation_pressure = float(np.clip(-forest_slope * 20, 0, 50))
        agri_expansion = float(np.clip(agri_slope * 20, 0, 50))
        score = deforestation_pressure + agri_expansion

        # Level penalty: very low remaining forest
        forest_latest = float(forest_arr[-1])
        if forest_latest < 10:
            score = min(100, score + 10)

        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "n_years": len(common),
            "forest_cover_pct_latest": round(forest_latest, 2),
            "agri_land_pct_latest": round(float(agri_arr[-1]), 2),
            "forest_trend_slope": round(float(forest_slope), 4),
            "agri_trend_slope": round(float(agri_slope), 4),
            "deforestation_pressure": round(deforestation_pressure, 2),
            "agri_expansion_pressure": round(agri_expansion, 2),
            "year_range": [common[0], common[-1]],
        }
