"""Fiscal space exhaustion: interest payments as % of revenue post-COVID.

Interest payments as a share of government revenue (WDI GC.XPN.INTP.RV.ZS)
measure the fiscal burden of debt service relative to available resources.
COVID-19 forced emergency borrowing, raising debt stocks and interest obligations.
Rising interest-to-revenue ratios compress fiscal space for public investment,
social spending, and crisis response.

IMF/World Bank debt distress thresholds: interest/revenue above 18% for
market-access countries, above 14% for low-income countries. Sri Lanka's 2022
default occurred when this ratio exceeded 70%.

Score: low burden (<10%) -> STABLE, moderate (10-20%) -> WATCH,
high (20-35%) -> STRESS, severe (>35%) -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class FiscalSpaceExhaustion(LayerBase):
    layer_id = "lPC"
    name = "Fiscal Space Exhaustion"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "GC.XPN.INTP.RV.ZS"
        name = "interest payments"
        rows = await db.fetch_all(
            "SELECT value, date FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for GC.XPN.INTP.RV.ZS",
            }

        values = [(r["date"], r["value"]) for r in rows if r["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "all null values"}

        values.sort(key=lambda x: x[0])
        latest = values[-1][1]

        pre_covid = [v for d, v in values if d < "2020-01-01"]
        baseline = pre_covid[-1] if pre_covid else values[0][1]
        increase_pp = latest - baseline

        # Score: higher interest/revenue = greater fiscal space exhaustion
        if latest < 10:
            score = 5.0 + latest * 1.0
        elif latest < 20:
            score = 15.0 + (latest - 10) * 2.5
        elif latest < 35:
            score = 40.0 + (latest - 20) * 2.0
        else:
            score = min(100.0, 70.0 + (latest - 35) * 1.0)

        # Additional penalty for sharp post-COVID increase
        if increase_pp > 10:
            score = min(100.0, score + 8.0)
        elif increase_pp > 5:
            score = min(100.0, score + 4.0)

        trend = round(values[-1][1] - values[0][1], 3) if len(values) > 1 else None

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "interest_revenue_ratio_pct": round(latest, 2),
                "pre_pandemic_baseline_pct": round(baseline, 2),
                "increase_post_covid_pp": round(increase_pp, 2),
                "above_imf_distress_threshold": latest > 18,
                "trend": trend,
                "n_obs": len(values),
            },
        }
