"""Value chain upgrading: manufacturing export sophistication proxy.

Countries that successfully upgrade within global value chains shift their
export mix toward higher value-added manufactures. The share of
manufacturing in merchandise exports (WDI TX.VAL.MANF.ZS.UN) serves as
a practical proxy for export sophistication.

Rising manufacturing share -> upgrading trajectory -> lower stress.
Declining manufacturing share -> downgrading / commodity dependence -> higher stress.

Score based on trend direction and latest level.
"""

import numpy as np

from app.layers.base import LayerBase


class ValueChainUpgrading(LayerBase):
    layer_id = "l1"
    name = "Value Chain Upgrading"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.wdi_code = 'TX.VAL.MANF.ZS.UN'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 2:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient manufacturing export data"}

        dates = []
        values = []
        for row in rows:
            val = row["value"]
            if val is None:
                continue
            dates.append(row["date"])
            values.append(float(val))

        if len(values) < 2:
            return {"score": None, "signal": "UNAVAILABLE", "error": "fewer than 2 valid manufacturing export observations"}

        arr = np.array(values)
        latest_share = float(arr[-1])
        mean_share = float(np.mean(arr))

        # OLS trend
        t = np.arange(len(arr), dtype=float)
        X = np.column_stack([np.ones(len(t)), t])
        beta = np.linalg.lstsq(X, arr, rcond=None)[0]
        trend_slope = float(beta[1])

        # Score: lower manufacturing share = more stress; declining trend = more stress
        # Base: invert latest share (0-100% -> 100-0 stress)
        level_score = max(0.0, min(70.0, (100.0 - latest_share) * 0.7))

        # Trend: declining trend adds up to 30 pts
        trend_score = max(0.0, min(30.0, -trend_slope * 3.0)) if trend_slope < 0 else 0.0

        score = float(np.clip(level_score + trend_score, 0.0, 100.0))

        # Upgrading trajectory classification
        if trend_slope > 0.5:
            trajectory = "upgrading"
        elif trend_slope < -0.5:
            trajectory = "downgrading"
        else:
            trajectory = "stable"

        return {
            "score": round(score, 2),
            "country": country,
            "latest_manuf_share_pct": round(latest_share, 4),
            "mean_manuf_share_pct": round(mean_share, 4),
            "trend_slope_pct_per_year": round(trend_slope, 6),
            "upgrading_trajectory": trajectory,
            "n_observations": len(values),
            "date_range": [dates[0], dates[-1]],
        }
