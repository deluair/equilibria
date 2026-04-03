"""Innovation Index module.

R&D intensity proxy: R&D expenditure as % of GDP.

Score formula: max(0, 3 - rnd_pct) * 33, clipped to 100.
Below 1% R&D/GDP = low innovation capacity.

Source: WDI (GB.XPD.RSDV.GD.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class InnovationIndex(LayerBase):
    layer_id = "lTE"
    name = "Innovation Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'GB.XPD.RSDV.GD.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no R&D expenditure data",
            }

        values = np.array([float(r["value"]) for r in rows])
        # Use most recent available value
        rnd_pct = float(values[-1])
        mean_rnd = float(np.mean(values))
        n_obs = len(values)

        score = float(np.clip(max(0.0, 3.0 - rnd_pct) * 33.0, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "rnd_pct_gdp_latest": round(rnd_pct, 3),
            "rnd_pct_gdp_mean": round(mean_rnd, 3),
            "n_obs": n_obs,
            "threshold_low": 1.0,
            "period": f"{rows[0]['date']} to {rows[-1]['date']}",
            "interpretation": "below 1% R&D/GDP signals low innovation capacity",
        }
