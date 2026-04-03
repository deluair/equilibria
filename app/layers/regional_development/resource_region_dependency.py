"""Resource Region Dependency module.

High natural-resource rents as a share of GDP indicate that extraction
activity -- which is geographically concentrated -- dominates the economy.
This creates regional concentration risk: regions without resources are
structurally dependent on transfers from resource-rich enclaves.

Score = clip(rents_pct * 2, 0, 100)
A 50 % rents/GDP ratio maps to the maximum score.

Sources: WDI NY.GDP.TOTL.RT.ZS (total natural resource rents % of GDP)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ResourceRegionDependency(LayerBase):
    layer_id = "lRD"
    name = "Resource Region Dependency"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.TOTL.RT.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        vals = [(r["date"], float(r["value"])) for r in rows if r["value"] is not None]
        if not vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid values"}

        rents = [v for _, v in vals]
        mean_rents = float(np.mean(rents))
        latest_rents = rents[0]
        latest_date = vals[0][0]

        score = float(np.clip(mean_rents * 2, 0, 100))

        trend = None
        if len(rents) >= 3:
            x = np.arange(len(rents), dtype=float)
            slope = float(np.polyfit(x, rents, 1)[0])
            trend = "increasing" if slope > 0.3 else ("decreasing" if slope < -0.3 else "stable")

        return {
            "score": round(score, 1),
            "country": country,
            "latest_date": latest_date,
            "latest_rents_pct": round(latest_rents, 2),
            "mean_rents_pct": round(mean_rents, 2),
            "n_obs": len(rents),
            "trend": trend,
            "series": "NY.GDP.TOTL.RT.ZS",
            "interpretation": "high rents = geographically concentrated resource extraction",
        }
