"""Path Dependency module.

Measures structural lock-in by tracking persistence in sector shares over time.
Very low annual change in agriculture or industry shares indicates path dependency
and low structural adaptability.

Score = clipped(100 - mean_annual_change * 10, 0, 100)
Near-zero change = maximum path dependency stress.

Sources: WDI NV.AGR.TOTL.ZS (agriculture % GDP), NV.IND.TOTL.ZS (industry % GDP)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_SECTOR_SERIES = {
    "agriculture": "NV.AGR.TOTL.ZS",
    "industry": "NV.IND.TOTL.ZS",
}


class PathDependency(LayerBase):
    layer_id = "lCP"
    name = "Path Dependency"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        sector_stats: dict[str, dict] = {}

        for label, series_id in _SECTOR_SERIES.items():
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
            if rows and len(rows) >= 5:
                vals = np.array([float(r["value"]) for r in rows])
                annual_changes = np.abs(np.diff(vals))
                mean_change = float(np.mean(annual_changes))
                sector_stats[label] = {
                    "mean_annual_change_pp": round(mean_change, 3),
                    "latest_share_pct": round(float(vals[-1]), 2),
                    "n_obs": len(vals),
                    "period": f"{rows[0]['date']} to {rows[-1]['date']}",
                }

        if not sector_stats:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        mean_changes = [v["mean_annual_change_pp"] for v in sector_stats.values()]
        overall_mean_change = float(np.mean(mean_changes))

        # Very low change = locked in path = high stress (score near 100)
        score = float(max(0.0, min(100.0, 100.0 - overall_mean_change * 10.0)))

        return {
            "score": round(score, 1),
            "country": country,
            "overall_mean_annual_sector_change_pp": round(overall_mean_change, 3),
            "sector_details": sector_stats,
            "interpretation": (
                "High score = low structural change = high path dependency (lock-in stress). "
                "Low score = active structural transformation = low lock-in."
            ),
            "_citation": "World Bank WDI: NV.AGR.TOTL.ZS, NV.IND.TOTL.ZS",
        }
