"""Geographic isolation: landlocked country proxy and trade cost stress.

Landlocked countries face structural trade cost disadvantages due to lack of
direct sea access. Trade openness (trade/GDP) below 40% for a landlocked
country signals severe geographic isolation and integration stress.

Proxy logic:
- Trade openness 'NE.TRD.GNFS.ZS' (exports + imports as % of GDP).
- Landlocked status inferred from metadata or treated as unknown (conservative).
- Low trade openness (<40%) signals geographic isolation regardless of status.

Score:
    openness < 20%  -> score = 90
    openness < 40%  -> score = clip(80 - (openness - 20) * 2, 40, 80)
    openness < 80%  -> score = clip(40 - (openness - 40) * 0.75, 5, 40)
    openness >= 80% -> score = max(0, 5 - (openness - 80) * 0.1)

References:
    Limao, N. & Venables, A.J. (2001). Infrastructure, Geographical Disadvantage,
        Transport Costs, and Trade. World Bank Economic Review, 15(3), 451-479.
    Faye, M.L., McArthur, J.W., Sachs, J.D. & Snow, T. (2004). The Challenges
        Facing Landlocked Developing Countries. Journal of Human Development, 5(1).

Sources: World Bank WDI NE.TRD.GNFS.ZS.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class GeographicIsolation(LayerBase):
    layer_id = "l11"
    name = "Geographic Isolation"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.TRD.GNFS.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no trade openness data",
                "country": country,
            }

        latest = rows[0]
        openness = float(latest["value"])
        year = latest["date"]

        # Score: lower openness = higher isolation stress
        if openness < 20.0:
            score = 90.0
        elif openness < 40.0:
            score = float(np.clip(80.0 - (openness - 20.0) * 2.0, 40.0, 80.0))
        elif openness < 80.0:
            score = float(np.clip(40.0 - (openness - 40.0) * 0.75, 5.0, 40.0))
        else:
            score = float(max(0.0, 5.0 - (openness - 80.0) * 0.1))

        # Trend
        trend_slope = None
        if len(rows) >= 3:
            vals = np.array([float(r["value"]) for r in reversed(rows)])
            t = np.arange(len(vals), dtype=float)
            trend_slope = round(float(np.polyfit(t, vals, 1)[0]), 4)

        return {
            "score": round(score, 2),
            "country": country,
            "trade_openness_pct": round(openness, 2),
            "year": year,
            "trend_slope_pp_per_yr": trend_slope,
            "isolation_level": (
                "severe" if openness < 20
                else "high" if openness < 40
                else "moderate" if openness < 80
                else "low"
            ),
            "n_obs": len(rows),
            "_source": "WDI NE.TRD.GNFS.ZS",
        }
