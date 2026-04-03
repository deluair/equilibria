"""Trade policy uncertainty: tariff rate volatility over time.

Tariff volatility is a direct measure of policy unpredictability. High
standard deviation of applied MFN tariff rates across years signals that
exporters and investors face uncertain trade costs, which suppresses trade
flows and investment (Handley & Limao 2015, Pierce & Schott 2016).

Indicator: TM.TAX.MRCH.WM.AR.ZS -- Tariff rate, applied, weighted mean,
all products (%).

Metrics:
- std dev of tariff over the available time series
- trend slope (rising = escalating policy regime)
- latest level

Score: higher volatility and rising trend = higher uncertainty = higher stress.
"""

import numpy as np

from app.layers.base import LayerBase


class TradePolicyUncertainty(LayerBase):
    layer_id = "l1"
    name = "Trade Policy Uncertainty"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.wdi_code = 'TM.TAX.MRCH.WM.AR.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient tariff data for uncertainty estimation"}

        dates = []
        values = []
        for row in rows:
            val = row["value"]
            if val is None:
                continue
            dates.append(row["date"])
            values.append(float(val))

        if len(values) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "fewer than 3 valid tariff observations"}

        arr = np.array(values)
        tariff_std = float(np.std(arr, ddof=1))
        latest_tariff = float(arr[-1])
        mean_tariff = float(np.mean(arr))

        # OLS trend
        t = np.arange(len(arr), dtype=float)
        X = np.column_stack([np.ones(len(t)), t])
        beta = np.linalg.lstsq(X, arr, rcond=None)[0]
        trend_slope = float(beta[1])

        # Score:
        # - Volatility component: std dev mapped 0-70 (std > 5pp = severe)
        vol_score = min(70.0, tariff_std * 14.0)

        # - Trend component: rising tariffs (escalation) add up to 20 pts
        trend_score = max(0.0, min(20.0, trend_slope * 10.0)) if trend_slope > 0 else 0.0

        # - Level component: high tariff level adds up to 10 pts
        level_score = min(10.0, latest_tariff * 0.5)

        score = float(np.clip(vol_score + trend_score + level_score, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "country": country,
            "tariff_std_pct": round(tariff_std, 4),
            "latest_tariff_pct": round(latest_tariff, 4),
            "mean_tariff_pct": round(mean_tariff, 4),
            "trend_slope_pct_per_year": round(trend_slope, 6),
            "trend_direction": "rising" if trend_slope > 0 else "falling",
            "n_observations": len(values),
            "date_range": [dates[0], dates[-1]],
        }
