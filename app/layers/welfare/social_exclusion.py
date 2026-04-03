"""Social Exclusion module.

Multi-dimensional exclusion index: unemployment + poverty + low primary
education enrollment. Each dimension captures a distinct pathway through
which individuals are excluded from mainstream social and economic life.

Indicators:
  - SL.UEM.TOTL.ZS  : unemployment rate (% of labor force)
  - SI.POV.DDAY      : poverty headcount at $2.15/day (%)
  - SE.PRM.NENR      : primary school net enrollment rate (%)

Score: weighted sum of exclusion penalties; low enrollment + high poverty +
high unemployment = severe social exclusion stress.

Sources: WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class SocialExclusion(LayerBase):
    layer_id = "lWE"
    name = "Social Exclusion"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        series_map = {
            "unemployment": "SL.UEM.TOTL.ZS",
            "poverty": "SI.POV.DDAY",
            "primary_enrollment": "SE.PRM.NENR",
        }

        latest: dict[str, float | None] = {}
        dates: dict[str, str | None] = {}

        for key, sid in series_map.items():
            rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                ORDER BY dp.date DESC
                LIMIT 1
                """,
                (country, sid),
            )
            if rows:
                latest[key] = float(rows[0]["value"])
                dates[key] = rows[0]["date"]
            else:
                latest[key] = None
                dates[key] = None

        available = {k: v for k, v in latest.items() if v is not None}
        if len(available) == 0:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no social exclusion dimension data available",
            }

        penalties: dict[str, float] = {}

        # Unemployment penalty: above 5% threshold, max 30 points
        if latest["unemployment"] is not None:
            penalties["unemployment"] = float(np.clip((latest["unemployment"] - 5.0) * 1.5, 0, 30))

        # Poverty penalty: direct % headcount contribution, max 40 points
        if latest["poverty"] is not None:
            penalties["poverty"] = float(np.clip(latest["poverty"] * 0.4, 0, 40))

        # Education exclusion: low enrollment -> stress; below 90% raises penalty, max 30 points
        if latest["primary_enrollment"] is not None:
            penalties["education"] = float(np.clip((90.0 - latest["primary_enrollment"]) * 1.0, 0, 30))

        if not penalties:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient dimension data for exclusion score",
            }

        score = float(np.clip(sum(penalties.values()), 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "unemployment_pct": round(latest["unemployment"], 2) if latest["unemployment"] is not None else None,
            "unemployment_date": dates["unemployment"],
            "poverty_headcount_pct": round(latest["poverty"], 2) if latest["poverty"] is not None else None,
            "poverty_date": dates["poverty"],
            "primary_enrollment_pct": round(latest["primary_enrollment"], 2) if latest["primary_enrollment"] is not None else None,
            "primary_enrollment_date": dates["primary_enrollment"],
            "exclusion_penalties": {k: round(v, 2) for k, v in penalties.items()},
            "n_dimensions": len(penalties),
            "method": "Additive exclusion penalties: unemployment + poverty + education gap",
            "reference": "Atkinson & Hills 1998; Silver 1994; EU Social Exclusion Framework",
        }
