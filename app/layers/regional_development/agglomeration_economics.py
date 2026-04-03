"""Agglomeration Economics module.

Measures the strength of agglomeration effects by combining urban
concentration, services-sector development, and GDP growth. Economies
with high urbanization and large services sectors that still grow slowly
exhibit agglomeration failure -- cities are not generating the productivity
spillovers theory predicts.

Sub-scores (each 0-100):
  urbanization_score = clip(urban_pct, 0, 100)          -- higher = more agglomeration potential
  services_score     = clip(srv_pct, 0, 100)            -- higher = more value-added concentration
  growth_score       = clip((growth + 5) * 5, 0, 100)   -- maps -5% -> 0, +15% -> 100

Agglomeration index = (urbanization_score + services_score + growth_score) / 3
Score = 100 - agglomeration_index  (high agglomeration potential + growth = low stress)

Sources: WDI SP.URB.TOTL.IN.ZS (urban population % of total),
         WDI NV.SRV.TOTL.ZS (services value added % of GDP),
         WDI NY.GDP.MKTP.KD.ZG (GDP growth % annual)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_SERIES = {
    "urban": "SP.URB.TOTL.IN.ZS",
    "services": "NV.SRV.TOTL.ZS",
    "gdp_growth": "NY.GDP.MKTP.KD.ZG",
}


class AgglomerationEconomics(LayerBase):
    layer_id = "lRD"
    name = "Agglomeration Economics"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        fetched = {}
        for label, series_id in _SERIES.items():
            rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                ORDER BY dp.date DESC
                LIMIT 5
                """,
                (country, series_id),
            )
            vals = [float(r["value"]) for r in rows if r["value"] is not None]
            if vals:
                fetched[label] = {"value": vals[0], "mean": float(np.mean(vals)), "date": rows[0]["date"]}

        if len(fetched) < 2:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        sub_scores = {}
        penalty_parts = []

        if "urban" in fetched:
            urban_score = float(np.clip(fetched["urban"]["mean"], 0, 100))
            sub_scores["urbanization"] = round(urban_score, 2)
            penalty_parts.append(100 - urban_score)

        if "services" in fetched:
            srv_score = float(np.clip(fetched["services"]["mean"], 0, 100))
            sub_scores["services"] = round(srv_score, 2)
            penalty_parts.append(100 - srv_score)

        if "gdp_growth" in fetched:
            g = fetched["gdp_growth"]["mean"]
            growth_score = float(np.clip((g + 5) * 5, 0, 100))
            sub_scores["gdp_growth"] = round(growth_score, 2)
            penalty_parts.append(100 - growth_score)

        score = float(np.clip(np.mean(penalty_parts), 0, 100))

        components = {
            label: {
                "latest": round(fetched[label]["value"], 2),
                "mean": round(fetched[label]["mean"], 2),
                "date": fetched[label]["date"],
            }
            for label in fetched
        }

        return {
            "score": round(score, 1),
            "country": country,
            "sub_scores": sub_scores,
            "components": components,
            "series": _SERIES,
            "interpretation": "low urban + low services + low growth = agglomeration failure",
        }
