"""Phase Transition Risk module.

Economic tipping point detection: simultaneous deterioration in multiple key indicators
within the last 3 years signals proximity to a phase transition (crisis).

Indicators: GDP growth decline, inflation surge, fiscal balance deterioration.
Each co-deterioration in last 3 years = one tipping signal.
Score = (n_signals / max_signals) * 100

Sources:
  WDI NY.GDP.MKTP.KD.ZG (GDP growth %)
  WDI FP.CPI.TOTL.ZG (CPI inflation %)
  WDI GC.BAL.CASH.GD.ZS (cash surplus/deficit % GDP)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_SERIES = {
    "gdp_growth": "NY.GDP.MKTP.KD.ZG",
    "inflation": "FP.CPI.TOTL.ZG",
    "fiscal_balance": "GC.BAL.CASH.GD.ZS",
}


class PhaseTransitionRisk(LayerBase):
    layer_id = "lCP"
    name = "Phase Transition Risk"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        series_data: dict[str, list[tuple[str, float]]] = {}

        for label, series_id in _SERIES.items():
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
                series_data[label] = [(r["date"], float(r["value"])) for r in rows]

        if not series_data:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        signals: list[str] = []
        details: dict = {}

        # Check each indicator for deterioration in last 3 obs vs prior 3 obs
        for label, data in series_data.items():
            vals = np.array([v for _, v in data])
            dates = [d for d, _ in data]

            if len(vals) < 6:
                continue

            recent = vals[-3:]
            prior = vals[-6:-3]
            recent_mean = float(np.mean(recent))
            prior_mean = float(np.mean(prior))

            # Deterioration: depends on indicator direction
            if label == "gdp_growth":
                # Deterioration = growth declining
                deteriorated = recent_mean < prior_mean
            elif label == "inflation":
                # Deterioration = inflation surging (>2pp increase)
                deteriorated = recent_mean > prior_mean + 2.0
            elif label == "fiscal_balance":
                # Deterioration = balance worsening (becoming more negative)
                deteriorated = recent_mean < prior_mean

            else:
                deteriorated = False

            details[label] = {
                "recent_3yr_mean": round(recent_mean, 3),
                "prior_3yr_mean": round(prior_mean, 3),
                "deteriorated": deteriorated,
                "latest_date": dates[-1],
            }

            if deteriorated:
                signals.append(label)

        n_signals = len(signals)
        max_signals = len(series_data)
        score = float(n_signals / max_signals * 100.0) if max_signals > 0 else 0.0

        return {
            "score": round(score, 1),
            "country": country,
            "n_tipping_signals": n_signals,
            "max_possible_signals": max_signals,
            "active_signals": signals,
            "indicator_details": details,
            "interpretation": (
                "Each simultaneous deterioration = one tipping signal. "
                "Score 100 = all indicators deteriorating concurrently (phase transition risk). "
                "Score 0 = no indicators deteriorating."
            ),
            "_citation": "World Bank WDI: NY.GDP.MKTP.KD.ZG, FP.CPI.TOTL.ZG, GC.BAL.CASH.GD.ZS",
        }
