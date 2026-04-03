"""Fertility rate transition: TFR vs replacement rate threshold.

Compares the total fertility rate (TFR) against the biological replacement
rate of 2.1. A TFR at or near replacement is demographically stable. Rapid
decline below replacement signals long-run population contraction; sustained
high TFR signals resource and human-capital strain.

Indicator: SP.DYN.TFRT.IN (World Bank WDI)
Replacement threshold: 2.1 children per woman
Optimal range: 2.0 - 2.5

Score (0-100): higher score = greater stress
    TFR in [2.0, 2.5] -> near-replacement (STABLE, score ~15)
    TFR < 1.5 -> deep sub-replacement (CRISIS, score -> 100)
    TFR > 5.0 -> very high fertility (STRESS, score -> 80+)

References:
    Becker, G.S. (1960). An Economic Analysis of Fertility. NBER.
    Notestein, F.W. (1945). Population: the long view. In T. Schultz (ed.),
        Food for the World. University of Chicago Press.
    United Nations (2022). World Population Prospects 2022. DESA.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

REPLACEMENT_RATE = 2.1
OPTIMAL_LOW = 2.0
OPTIMAL_HIGH = 2.5


class FertilityRateTransition(LayerBase):
    layer_id = "lDT"
    name = "Fertility Rate Transition"

    async def compute(self, db, **kwargs) -> dict:
        code = "SP.DYN.TFRT.IN"
        name = "fertility rate total"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no TFR data"}

        values = [row["value"] for row in rows if row["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no TFR data"}

        latest_tfr = float(values[0])
        avg_tfr = float(np.mean(values))
        trend = float(values[-1] - values[0]) if len(values) > 1 else 0.0

        score = _tfr_to_score(latest_tfr)
        score = float(np.clip(score, 0, 100))

        gap_from_replacement = round(latest_tfr - REPLACEMENT_RATE, 3)
        at_replacement = OPTIMAL_LOW <= latest_tfr <= OPTIMAL_HIGH

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "latest_tfr": round(latest_tfr, 3),
            "replacement_rate": REPLACEMENT_RATE,
            "gap_from_replacement": gap_from_replacement,
            "at_replacement": at_replacement,
            "avg_tfr_15y": round(avg_tfr, 3),
            "trend_direction": "declining" if trend < -0.05 else "rising" if trend > 0.05 else "stable",
            "n_obs": len(values),
            "indicator": code,
        }


def _tfr_to_score(tfr: float) -> float:
    if OPTIMAL_LOW <= tfr <= OPTIMAL_HIGH:
        return 15.0
    if 1.5 <= tfr < OPTIMAL_LOW:
        return 15.0 + (OPTIMAL_LOW - tfr) * 60.0
    if tfr < 1.5:
        return 45.0 + (1.5 - tfr) * 110.0
    if OPTIMAL_HIGH < tfr <= 4.0:
        return 15.0 + (tfr - OPTIMAL_HIGH) * 16.7
    return min(100.0, 40.0 + (tfr - 4.0) * 20.0)
