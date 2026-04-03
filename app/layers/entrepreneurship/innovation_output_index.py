"""Innovation Output Index module.

Measures innovation output using:
- IP.PAT.RESD.IR: Patent applications, residents (per million population proxy via
  IP.PAT.RESD and SP.POP.TOTL)
- GB.XPD.RSDV.GD.ZS: R&D expenditure (% GDP) -- input that drives output

Patents per capita and R&D intensity jointly capture the economy's capacity to
generate new knowledge and translate it into commercializable outputs. Low values
signal weak innovation infrastructure and limited entrepreneurial knowledge spillovers.

Score: higher score = lower innovation output = more stress.

Sources: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class InnovationOutputIndex(LayerBase):
    layer_id = "lER"
    name = "Innovation Output Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        patent_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IP.PAT.RESD'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        pop_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SP.POP.TOTL'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )

        rnd_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GB.XPD.RSDV.GD.ZS'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        patents_per_million: float | None = None
        rnd_pct_gdp: float | None = None

        if patent_rows and pop_rows:
            patent_vals = [float(r["value"]) for r in patent_rows if r["value"] is not None]
            pop = float(pop_rows[0]["value"])
            if patent_vals and pop > 0:
                avg_patents = float(np.mean(patent_vals))
                patents_per_million = (avg_patents / pop) * 1_000_000

        if rnd_rows:
            vals = [float(r["value"]) for r in rnd_rows if r["value"] is not None]
            rnd_pct_gdp = float(np.mean(vals)) if vals else None

        if patents_per_million is None and rnd_pct_gdp is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no patent or R&D data available"}

        score_parts: list[float] = []

        if patents_per_million is not None:
            # Patents per million: 0-1000+ for frontier innovators. Clamp at 500.
            norm = min(100.0, (patents_per_million / 500.0) * 100.0)
            score_parts.append(max(0.0, 100.0 - norm))

        if rnd_pct_gdp is not None:
            # R&D: 0-5% GDP. Higher = lower stress. Clamp at 4%.
            norm = min(100.0, (rnd_pct_gdp / 4.0) * 100.0)
            score_parts.append(max(0.0, 100.0 - norm))

        score = float(np.mean(score_parts))

        return {
            "score": round(score, 1),
            "country": country,
            "patents_per_million_pop": round(patents_per_million, 2) if patents_per_million is not None else None,
            "rnd_pct_gdp": round(rnd_pct_gdp, 3) if rnd_pct_gdp is not None else None,
            "interpretation": "High score = few patents + low R&D = weak innovation output",
        }
