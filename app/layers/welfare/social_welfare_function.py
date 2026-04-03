"""Social Welfare Function module.

Utilitarian social welfare following Sen (1976) and Atkinson (1970):
  W = mean_income * (1 - Gini/100)

This integrates efficiency (income level) and equity (inequality) into a
single welfare metric. Declining W over time signals welfare loss.

Indicators:
  - NY.GDP.PCAP.KD : GDP per capita, constant 2015 USD
  - SI.POV.GINI    : Gini coefficient

Score = 100 - (W_latest / W_max_historical) * 100, where declining
welfare relative to historical peak raises the stress score.

Sources: WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class SocialWelfareFunction(LayerBase):
    layer_id = "lWE"
    name = "Social Welfare Function"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        income_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.PCAP.KD'
            ORDER BY dp.date
            """,
            (country,),
        )

        gini_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SI.POV.GINI'
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )

        if not income_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no income data available",
            }

        if not gini_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no Gini data available",
            }

        income_vals = np.array([float(r["value"]) for r in income_rows])
        gini = float(gini_rows[0]["value"])
        gini_date = gini_rows[0]["date"]
        income_latest = float(income_vals[-1])
        income_date = income_rows[-1]["date"]

        equity_factor = 1.0 - (gini / 100.0)

        # W = income * equity
        # For historical W, use mean income * equity (Gini series is sparse; use latest Gini)
        w_latest = income_latest * equity_factor
        w_mean = float(np.mean(income_vals)) * equity_factor
        w_max = float(np.max(income_vals)) * equity_factor

        # W_latest relative to historical peak
        if w_max > 0:
            w_ratio = float(w_latest / w_max)
        else:
            w_ratio = 1.0

        # Score: loss relative to peak (0 = at peak, 100 = fully collapsed)
        score = float(np.clip((1.0 - w_ratio) * 100, 0, 100))

        # Trend: compare last 5 obs to first 5 obs
        welfare_trend = None
        if len(income_vals) >= 10:
            w_early = float(np.mean(income_vals[:5])) * equity_factor
            w_recent = float(np.mean(income_vals[-5:])) * equity_factor
            welfare_trend = round(float((w_recent - w_early) / max(abs(w_early), 1.0) * 100), 2)

        return {
            "score": round(score, 1),
            "country": country,
            "gini": round(gini, 2),
            "gini_date": gini_date,
            "income_per_capita_usd": round(income_latest, 2),
            "income_date": income_date,
            "welfare_w_latest": round(w_latest, 2),
            "welfare_w_mean": round(w_mean, 2),
            "welfare_w_max": round(w_max, 2),
            "welfare_ratio_vs_peak": round(w_ratio, 4),
            "welfare_trend_pct_change": welfare_trend,
            "equity_factor": round(equity_factor, 4),
            "n_income_obs": len(income_rows),
            "method": "W = mean_income * (1 - Gini/100); score = (1 - W_latest/W_max) * 100",
            "reference": "Sen 1976; Atkinson 1970; Dagum 1990",
        }
