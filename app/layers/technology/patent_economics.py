"""Patent Economics module.

Patent intensity proxy via R&D expenditure and patent applications.

Score = max(0, 2 - rnd_pct) * 50, clipped to 100.
Low R&D = low patent potential.

Uses IP.PAT.RESD.IR.D5Y if available; falls back to R&D as sole indicator.

Sources: WDI (GB.XPD.RSDV.GD.ZS, IP.PAT.RESD.IR.D5Y)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PatentEconomics(LayerBase):
    layer_id = "lTE"
    name = "Patent Economics"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rnd_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'GB.XPD.RSDV.GD.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )
        patent_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'IP.PAT.RESD.IR.D5Y'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rnd_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no R&D expenditure data",
            }

        rnd_vals = np.array([float(r["value"]) for r in rnd_rows])
        rnd_latest = float(rnd_vals[-1])
        rnd_mean = float(np.mean(rnd_vals))

        # Primary score from R&D: max(0, 2 - rnd_pct) * 50, clipped to 100
        rnd_score = float(np.clip(max(0.0, 2.0 - rnd_latest) * 50.0, 0.0, 100.0))

        patent_latest = None
        patent_adjustment = 0.0

        if patent_rows:
            patent_vals = np.array([float(r["value"]) for r in patent_rows])
            patent_latest = float(patent_vals[-1])
            # Adjust score downward (better) if patent activity is high relative to R&D
            # High patent activity with moderate R&D = efficient innovation
            patent_per_rnd = patent_latest / (rnd_latest + 0.1)
            if patent_per_rnd > 50:
                patent_adjustment = -min(20.0, (patent_per_rnd - 50.0) * 0.2)
            elif patent_per_rnd < 5:
                patent_adjustment = min(15.0, (5.0 - patent_per_rnd) * 1.5)

        score = float(np.clip(rnd_score + patent_adjustment, 0.0, 100.0))

        result = {
            "score": round(score, 1),
            "country": country,
            "rnd_pct_gdp_latest": round(rnd_latest, 3),
            "rnd_pct_gdp_mean": round(rnd_mean, 3),
            "rnd_n_obs": len(rnd_rows),
            "rnd_score_base": round(rnd_score, 1),
            "interpretation": "low R&D = low patent potential; score = max(0, 2 - rnd_pct) * 50",
        }
        if patent_latest is not None:
            result["patent_applications_latest"] = round(patent_latest, 1)
            result["patent_adjustment"] = round(patent_adjustment, 1)
            result["patent_n_obs"] = len(patent_rows)

        return result
