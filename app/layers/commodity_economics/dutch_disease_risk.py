"""Dutch Disease Risk module.

Measures whether a booming resource sector crowds out manufacturing by
appreciating the real exchange rate and reducing manufacturing value added.

Methodology:
- Query natural resource rents as % GDP (NY.GDP.TOTL.RT.ZS).
- Query manufacturing value added as % GDP (NV.IND.MANF.ZS).
- Query real effective exchange rate (PX.REX.REER) trend.
- Dutch disease proxy: high resource rents + low/declining manufacturing share
  + appreciating REER.
- Score = clip(resource_rents * 1.5 + max(0, 20 - manuf_share) * 1.0
             + reer_appreciation_penalty, 0, 100).

Sources: World Bank WDI (NY.GDP.TOTL.RT.ZS, NV.IND.MANF.ZS, PX.REX.REER).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class DutchDiseaseRisk(LayerBase):
    layer_id = "lCM"
    name = "Dutch Disease Risk"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        async def _latest(series_id: str) -> float | None:
            rows = await db.fetch_all(
                """
                SELECT dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                ORDER BY dp.date DESC
                LIMIT 1
                """,
                (country, series_id),
            )
            return float(rows[0]["value"]) if rows else None

        async def _series_slope(series_id: str, n: int = 10) -> float:
            rows = await db.fetch_all(
                """
                SELECT dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                ORDER BY dp.date DESC
                LIMIT ?
                """,
                (country, series_id, n),
            )
            if len(rows) < 5:
                return 0.0
            vals = [float(r["value"]) for r in reversed(rows)]
            t = np.arange(len(vals), dtype=float)
            coeffs = np.polyfit(t, vals, 1)
            return float(coeffs[0])

        resource_rents = await _latest("NY.GDP.TOTL.RT.ZS")
        manuf_share = await _latest("NV.IND.MANF.ZS")
        reer_slope = await _series_slope("PX.REX.REER")

        if resource_rents is None and manuf_share is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        rents = resource_rents or 0.0
        manuf = manuf_share or 15.0  # assume moderate if missing

        reer_penalty = min(max(reer_slope * 2, 0), 15)

        score = float(np.clip(
            rents * 1.5 + max(0.0, 20.0 - manuf) * 1.0 + reer_penalty,
            0,
            100,
        ))

        return {
            "score": round(score, 1),
            "country": country,
            "resource_rents_pct_gdp": round(rents, 3),
            "manufacturing_share_pct_gdp": round(manuf, 3),
            "reer_trend_slope": round(reer_slope, 3),
            "crowding_out_risk": rents > 10 and manuf < 15,
            "indicators": ["NY.GDP.TOTL.RT.ZS", "NV.IND.MANF.ZS", "PX.REX.REER"],
        }
