"""Remittance Dependence module.

Measures remittance inflows as a percentage of GDP to assess economic
vulnerability from over-reliance on migrant worker transfers.

High remittance dependence (>10% GDP) signals vulnerability to
external shocks and reduced domestic labor force. Above 25% indicates
extreme dependence where a migration shock can destabilize the economy.

Score = clip(remittance_pct * 3, 0, 100)

Sources: WDI (BX.TRF.PWKR.DT.GD.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class RemittanceDependence(LayerBase):
    layer_id = "lME"
    name = "Remittance Dependence"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'BX.TRF.PWKR.DT.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        values = [float(r["value"]) for r in rows if r["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        latest = values[0]
        avg = float(np.mean(values))

        score = float(np.clip(latest * 3, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "remittance_pct_gdp_latest": round(latest, 2),
            "remittance_pct_gdp_avg": round(avg, 2),
            "n_obs": len(values),
            "period": f"{rows[-1]['date']} to {rows[0]['date']}",
            "thresholds": {"concern": 10.0, "high_dependence": 25.0},
            "interpretation": (
                "high dependence" if latest > 25
                else "concern" if latest > 10
                else "moderate"
            ),
        }
