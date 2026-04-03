"""Pandemic output loss: cumulative GDP gap relative to pre-pandemic trend (2020-2023).

The output loss from COVID-19 is measured as the deviation of actual GDP growth
from the pre-pandemic trend (2015-2019 average annual growth). Larger cumulative
gaps indicate deeper scarring and slower recovery trajectories.

IMF (2022) estimates global cumulative output loss at ~$13.8 trillion through 2024.
Persistent gaps signal hysteresis: demand collapse, investment shortfall, or
permanent labor force exits that prevent return to trend.

Score: small gap (<5% cumulative) -> STABLE, moderate (5-15%) -> WATCH,
large (15-30%) -> STRESS, severe (>30%) -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class PandemicOutputLoss(LayerBase):
    layer_id = "lPC"
    name = "Pandemic Output Loss"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "NY.GDP.MKTP.KD.ZG"
        name = "GDP growth"
        rows = await db.fetch_all(
            "SELECT value, date FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for NY.GDP.MKTP.KD.ZG"}

        values = [(r["date"], r["value"]) for r in rows if r["value"] is not None]
        if len(values) < 4:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient GDP growth observations"}

        # Sort ascending by date
        values.sort(key=lambda x: x[0])
        growth_rates = [v for _, v in values]

        # Pre-pandemic trend: average growth before 2020 (last 5 obs before COVID window)
        pre_pandemic = [v for d, v in values if d < "2020-01-01"]
        covid_era = [v for d, v in values if "2020-01-01" <= d <= "2023-12-31"]

        if not pre_pandemic:
            # Fall back: use oldest half as pre-pandemic proxy
            mid = len(growth_rates) // 2
            pre_pandemic = growth_rates[:mid]
            covid_era = growth_rates[mid:]

        trend = sum(pre_pandemic) / len(pre_pandemic)
        actual_avg = sum(covid_era) / len(covid_era) if covid_era else growth_rates[-1]

        # Cumulative gap: trend minus actual, summed over covid era years
        n_years = max(len(covid_era), 1)
        cumulative_gap = (trend - actual_avg) * n_years

        # Score: higher cumulative gap = worse recovery
        if cumulative_gap < 0:
            score = 5.0  # Outperformed trend
        elif cumulative_gap < 5:
            score = 5.0 + cumulative_gap * 3.0
        elif cumulative_gap < 15:
            score = 20.0 + (cumulative_gap - 5) * 3.0
        elif cumulative_gap < 30:
            score = 50.0 + (cumulative_gap - 15) * 2.0
        else:
            score = min(100.0, 80.0 + (cumulative_gap - 30) * 1.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "pre_pandemic_trend_growth_pct": round(trend, 3),
                "covid_era_avg_growth_pct": round(actual_avg, 3),
                "cumulative_gap_pct": round(cumulative_gap, 3),
                "n_pre_pandemic_obs": len(pre_pandemic),
                "n_covid_era_obs": len(covid_era),
            },
        }
