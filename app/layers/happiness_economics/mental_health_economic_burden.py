"""Mental health economic burden: disorder prevalence and productivity loss.

The World Health Organization estimates that depression and anxiety alone cost
the global economy $1 trillion annually in lost productivity. Mental health
disorders account for a disproportionate share of disability-adjusted life years
(DALYs) in middle- and high-income countries. The economic burden encompasses
absenteeism, presenteeism, premature retirement, and direct treatment costs.

This module proxies mental health burden via WDI/WHO indicators: suicide
mortality rate (SH.STA.SUIC.P5) as a severe mental health outcome proxy and
health expenditure share (SH.XPD.CHEX.GD.ZS) as system response capacity.

Score: low suicide rate + adequate health spend -> STABLE, high suicide rate +
low health spend -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase

# WHO: global suicide rate ~9/100k. High-income avg ~12. High burden >20/100k.
_NORM_RATE = 9.0
_HIGH_RATE = 20.0


class MentalHealthEconomicBurden(LayerBase):
    layer_id = "lHE"
    name = "Mental Health Economic Burden"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        suicide_code = "SH.STA.SUIC.P5"
        he_code = "SH.XPD.CHEX.GD.ZS"

        suicide_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (suicide_code, "%suicide%mortality%"),
        )
        he_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (he_code, "%health expenditure%"),
        )

        suicide_vals = [r["value"] for r in suicide_rows if r["value"] is not None]
        he_vals = [r["value"] for r in he_rows if r["value"] is not None]

        if not suicide_vals and not he_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for SH.STA.SUIC.P5 or SH.XPD.CHEX.GD.ZS",
            }

        score_parts = []

        if suicide_vals:
            rate = suicide_vals[0]
            # Score: low rate -> STABLE, high rate -> CRISIS
            if rate < 5:
                sr_score = 8.0 + rate * 1.4
            elif rate < 10:
                sr_score = 15.0 + (rate - 5) * 3.0
            elif rate < 20:
                sr_score = 30.0 + (rate - 10) * 2.5
            elif rate < 30:
                sr_score = 55.0 + (rate - 20) * 2.0
            else:
                sr_score = min(100.0, 75.0 + (rate - 30) * 1.2)
            score_parts.append(sr_score)

        if he_vals:
            # Low health spend -> reduced capacity to address mental health
            he = he_vals[0]
            # <3% GDP: very low capacity, >10%: strong capacity
            he_score = max(5.0, 65.0 - he * 6.0)
            score_parts.append(min(100.0, he_score))

        score = sum(score_parts) / len(score_parts)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "suicide_rate_per_100k": round(suicide_vals[0], 2) if suicide_vals else None,
                "health_expenditure_gdp_pct": round(he_vals[0], 2) if he_vals else None,
                "who_norm_suicide_rate": _NORM_RATE,
                "burden_tier": (
                    "low"
                    if score < 25
                    else "moderate"
                    if score < 50
                    else "high"
                    if score < 75
                    else "severe"
                ),
                "n_obs_suicide": len(suicide_vals),
                "n_obs_he": len(he_vals),
            },
        }
