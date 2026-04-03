"""Status Quo Bias module.

Structural change inertia: sector share persistence over 10+ years.
Very low annual change in sector shares (agriculture, industry, services) implies
resistance to structural transformation -- a form of status quo bias.

Score = max(0, 3 - mean_annual_change) * 25

Sources: WDI NV.AGR.TOTL.ZS, NV.IND.TOTL.ZS, NV.SRV.TOTL.ZS
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class StatusQuoBias(LayerBase):
    layer_id = "l13"
    name = "Status Quo Bias"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        indicators = {
            "agriculture": "NV.AGR.TOTL.ZS",
            "industry": "NV.IND.TOTL.ZS",
            "services": "NV.SRV.TOTL.ZS",
        }

        sector_data = {}
        for sector, series_id in indicators.items():
            rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                ORDER BY dp.date
                """,
                (country, series_id),
            )
            if rows and len(rows) >= 10:
                sector_data[sector] = np.array([float(r["value"]) for r in rows])

        if not sector_data:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient sector data"}

        annual_changes = []
        sector_summary = {}
        for sector, values in sector_data.items():
            diffs = np.abs(np.diff(values))
            mean_change = float(np.mean(diffs))
            annual_changes.append(mean_change)
            sector_summary[sector] = {
                "mean_annual_change_pct": round(mean_change, 3),
                "n_obs": len(values),
            }

        mean_annual_change = float(np.mean(annual_changes))
        score = float(np.clip(max(0.0, 3.0 - mean_annual_change) * 25, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "mean_annual_sector_change_pct": round(mean_annual_change, 3),
            "sectors": sector_summary,
            "interpretation": "Low annual sector share change indicates structural inertia / status quo bias",
        }
