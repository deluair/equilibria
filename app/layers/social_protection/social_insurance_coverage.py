"""Social Insurance Coverage module.

Social insurance (pensions + unemployment benefits) coverage proxy.

Queries 'GC.XPN.TRFT.ZS' (social transfers as % of government expenditure).
Low transfers signal a coverage gap in social insurance systems.

Score = clip(max(0, 30 - transfer_share) * 2, 0, 100)

Sources: WDI (GC.XPN.TRFT.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class SocialInsuranceCoverage(LayerBase):
    layer_id = "lSP"
    name = "Social Insurance Coverage"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.XPN.TRFT.ZS'
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

        transfer_share = float(np.mean(values))
        latest = float(values[0])

        score = float(np.clip(max(0.0, 30.0 - transfer_share) * 2.0, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "transfer_share_mean": round(transfer_share, 2),
            "transfer_share_latest": round(latest, 2),
            "n_obs": len(values),
            "period": f"{rows[-1]['date']} to {rows[0]['date']}",
            "interpretation": (
                "Low social transfers as % of govt expenditure indicates "
                "weak coverage of pension and unemployment insurance systems."
            ),
            "_series": "GC.XPN.TRFT.ZS",
            "_source": "WDI",
        }
