"""Minimum Wage Adequacy module.

Minimum wage adequacy: worker productivity combined with poverty incidence.

Queries:
- 'SL.GDP.PCAP.EM.KD' (GDP per person employed, constant 2017 USD)
- 'SI.POV.DDAY' (poverty headcount ratio at $2.15/day, % of population)

Low worker productivity + high poverty = inadequate minimum wage floor.

Score = clip(poverty_headcount * (1 - worker_productivity_norm), 0, 100)

where worker_productivity_norm maps log(SL.GDP.PCAP.EM.KD) to [0, 1].

Sources: WDI (SL.GDP.PCAP.EM.KD, SI.POV.DDAY)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

# Reference range for normalization: log of global min/max GDP per worker (approx)
_LOG_GDP_WORKER_MIN = np.log(500.0)    # ~USD 500 (low-income floor)
_LOG_GDP_WORKER_MAX = np.log(200000.0)  # ~USD 200k (high-income ceiling)


class MinimumWageAdequacy(LayerBase):
    layer_id = "lSP"
    name = "Minimum Wage Adequacy"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        prod_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SL.GDP.PCAP.EM.KD'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        pov_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SI.POV.DDAY'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not prod_rows or not pov_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        prod_vals = [float(r["value"]) for r in prod_rows if r["value"] is not None]
        pov_vals = [float(r["value"]) for r in pov_rows if r["value"] is not None]

        if not prod_vals or not pov_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        worker_productivity = float(np.mean(prod_vals))
        poverty_headcount = float(np.mean(pov_vals))

        # Normalize productivity via log scale
        log_prod = np.log(max(worker_productivity, 1.0))
        worker_productivity_norm = float(
            np.clip(
                (log_prod - _LOG_GDP_WORKER_MIN) / (_LOG_GDP_WORKER_MAX - _LOG_GDP_WORKER_MIN),
                0.0,
                1.0,
            )
        )

        score = float(np.clip(poverty_headcount * (1.0 - worker_productivity_norm), 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "gdp_per_worker_usd": round(worker_productivity, 2),
            "worker_productivity_norm": round(worker_productivity_norm, 4),
            "poverty_headcount_pct": round(poverty_headcount, 2),
            "n_obs_productivity": len(prod_vals),
            "n_obs_poverty": len(pov_vals),
            "interpretation": (
                "Low worker productivity combined with high poverty incidence signals "
                "an inadequate minimum wage floor relative to subsistence needs."
            ),
            "_series": ["SL.GDP.PCAP.EM.KD", "SI.POV.DDAY"],
            "_source": "WDI",
        }
