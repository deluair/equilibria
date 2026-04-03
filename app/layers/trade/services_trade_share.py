"""Services trade share: services exports as % of total exports.

Modern economies derive increasing value from knowledge-intensive services
exports (finance, IT, business services, tourism). Low and declining services
trade share signals structural stress: over-reliance on goods exports,
limited export sophistication, and vulnerability to goods-sector shocks.

Indicator: TX.VAL.SERV.ZS.WT -- Service exports as % of total exports of
goods and services (WDI).

Score: low share -> higher stress; declining trend -> additional penalty.
"""

import numpy as np

from app.layers.base import LayerBase


class ServicesTradeShare(LayerBase):
    layer_id = "l1"
    name = "Services Trade Share"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.wdi_code = 'TX.VAL.SERV.ZS.WT'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 2:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient services export data"}

        dates = []
        values = []
        for row in rows:
            val = row["value"]
            if val is None:
                continue
            dates.append(row["date"])
            values.append(float(val))

        if len(values) < 2:
            return {"score": None, "signal": "UNAVAILABLE", "error": "fewer than 2 valid services trade observations"}

        arr = np.array(values)
        latest_share = float(arr[-1])
        mean_share = float(np.mean(arr))

        # OLS trend
        t = np.arange(len(arr), dtype=float)
        X = np.column_stack([np.ones(len(t)), t])
        beta = np.linalg.lstsq(X, arr, rcond=None)[0]
        trend_slope = float(beta[1])

        # Score:
        # Level: low services share -> stress. Global avg ~25%. Share <10% = high stress.
        # Map 0-50% share inversely to 0-70 stress points
        level_score = float(np.clip((50.0 - latest_share) / 50.0 * 70.0, 0.0, 70.0))

        # Trend: declining share adds up to 30 pts
        trend_score = max(0.0, min(30.0, -trend_slope * 3.0)) if trend_slope < 0 else 0.0

        score = float(np.clip(level_score + trend_score, 0.0, 100.0))

        # Classification
        if latest_share >= 40.0:
            level_label = "services-dominant"
        elif latest_share >= 20.0:
            level_label = "balanced goods-services"
        elif latest_share >= 10.0:
            level_label = "goods-dominant"
        else:
            level_label = "heavily goods-dependent"

        return {
            "score": round(score, 2),
            "country": country,
            "latest_services_share_pct": round(latest_share, 4),
            "mean_services_share_pct": round(mean_share, 4),
            "trend_slope_pct_per_year": round(trend_slope, 6),
            "trend_direction": "rising" if trend_slope > 0 else "declining",
            "export_structure": level_label,
            "n_observations": len(values),
            "date_range": [dates[0], dates[-1]],
        }
