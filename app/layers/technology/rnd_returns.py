"""R&D Returns module.

Returns on R&D investment: R&D expenditure (% GDP) vs per-capita GDP growth.

High R&D with low productivity growth = poor returns on innovation investment.

Sources: WDI (GB.XPD.RSDV.GD.ZS, NY.GDP.PCAP.KD.ZG)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class RnDReturns(LayerBase):
    layer_id = "lTE"
    name = "R&D Returns"

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
        prod_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'NY.GDP.PCAP.KD.ZG'
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
        if not prod_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no productivity growth data",
            }

        rnd_vals = np.array([float(r["value"]) for r in rnd_rows])
        prod_vals = np.array([float(r["value"]) for r in prod_rows])

        rnd_mean = float(np.mean(rnd_vals))
        prod_mean = float(np.mean(prod_vals))
        rnd_latest = float(rnd_vals[-1])
        prod_latest = float(prod_vals[-1])

        # Return efficiency ratio: productivity per unit of R&D
        # High R&D (>1.5%) but low productivity growth (<2%) = poor returns
        rnd_penalty = 0.0
        if rnd_mean > 0.5:
            expected_prod = rnd_mean * 1.5  # rough expected multiplier
            shortfall = max(0.0, expected_prod - prod_mean)
            rnd_penalty = float(np.clip(shortfall * 10.0, 0.0, 60.0))

        # Low R&D absolute level also signals weak innovation base
        low_rnd_penalty = float(np.clip(max(0.0, 2.0 - rnd_mean) * 20.0, 0.0, 40.0))

        score = float(np.clip(rnd_penalty + low_rnd_penalty, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "rnd_pct_gdp_latest": round(rnd_latest, 3),
            "rnd_pct_gdp_mean": round(rnd_mean, 3),
            "productivity_growth_latest_pct": round(prod_latest, 3),
            "productivity_growth_mean_pct": round(prod_mean, 3),
            "rnd_n_obs": len(rnd_rows),
            "prod_n_obs": len(prod_rows),
            "interpretation": "high R&D + low productivity growth = poor R&D returns",
        }
