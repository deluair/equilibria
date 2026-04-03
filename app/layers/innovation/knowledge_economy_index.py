"""Knowledge Economy Index module.

Measures the degree to which a country operates as a knowledge-based economy,
combining three dimensions:
- Internet users as % of population (IT.NET.USER.ZS)
- Education expenditure as % of GDP (SE.XPD.TOTL.GD.ZS)
- R&D expenditure as % of GDP (GB.XPD.RSDV.GD.ZS)

Score = 100 - normalized_composite, so higher score = weaker knowledge economy
(further from the knowledge frontier). A country near the frontier scores near 0.

Sources: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class KnowledgeEconomyIndex(LayerBase):
    layer_id = "lNV"
    name = "Knowledge Economy Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        series_map = {
            "IT.NET.USER.ZS": ("internet_pct", 100.0),
            "SE.XPD.TOTL.GD.ZS": ("edu_exp_pct_gdp", 10.0),
            "GB.XPD.RSDV.GD.ZS": ("rnd_pct_gdp", 5.0),
        }

        values: dict[str, float | None] = {}
        for sid in series_map:
            rows = await db.fetch_all(
                """
                SELECT dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                  AND dp.value IS NOT NULL
                ORDER BY dp.date DESC
                LIMIT 5
                """,
                (country, sid),
            )
            if rows:
                vals = [float(r["value"]) for r in rows if r["value"] is not None]
                values[sid] = float(np.mean(vals)) if vals else None
            else:
                values[sid] = None

        normed: list[float] = []
        components: dict[str, float | None] = {}
        for sid, (label, max_val) in series_map.items():
            raw = values.get(sid)
            components[label] = round(raw, 4) if raw is not None else None
            if raw is not None:
                normed.append(min(100.0, max(0.0, (raw / max_val) * 100.0)))

        if len(normed) < 2:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        normalized_composite = float(np.mean(normed))
        score = max(0.0, 100.0 - normalized_composite)

        return {
            "score": round(score, 1),
            "country": country,
            "normalized_composite": round(normalized_composite, 2),
            "components": components,
            "n_dimensions": len(normed),
            "interpretation": (
                "Score near 0 = strong knowledge economy; "
                "score near 100 = far from knowledge frontier"
            ),
        }
