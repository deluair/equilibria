"""Financial market concentration analysis.

Top-5 bank asset concentration (FB.BNK.COAS.ZS from Global Findex/FinStats)
measures oligopolistic structure in the financial sector. High concentration
can mean systemic 'too-big-to-fail' risk, but also can imply stable large
institutions. Interpreted in conjunction with banking stability signals.

Score (0-100): very high concentration (>90%) = systemic risk = stress.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MarketConcentrationFinance(LayerBase):
    layer_id = "lCK"
    name = "Financial Market Concentration"

    # Thresholds (Herfindahl-inspired, concentration %)
    LOW_CONC_THRESHOLD = 50.0   # competitive
    HIGH_CONC_THRESHOLD = 80.0  # oligopolistic

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        lookback = kwargs.get("lookback_years", 10)

        rows = await db.fetch_all(
            """
            SELECT ds.indicator_code, dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.indicator_code IN ('FB.BNK.COAS.ZS', 'GFDD.OI.01')
              AND ds.country_iso3 = ?
              AND dp.date >= date('now', ?)
            ORDER BY ds.indicator_code, dp.date
            """,
            (country, f"-{lookback} years"),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no bank concentration data",
            }

        by_code: dict[str, list[float]] = {}
        for r in rows:
            by_code.setdefault(r["indicator_code"], []).append(float(r["value"]))

        if "FB.BNK.COAS.ZS" in by_code:
            vals = np.array(by_code["FB.BNK.COAS.ZS"])
            indicator_used = "FB.BNK.COAS.ZS"
        elif "GFDD.OI.01" in by_code:
            vals = np.array(by_code["GFDD.OI.01"])
            indicator_used = "GFDD.OI.01"
        else:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no bank asset concentration series found",
            }

        conc_latest = float(vals[-1])
        conc_mean = float(np.mean(vals))
        conc_trend = float(vals[-1] - vals[0]) if len(vals) > 1 else 0.0

        # Non-linear score: moderate concentration (50-70%) is normal; extremes = stress
        if conc_latest < self.LOW_CONC_THRESHOLD:
            score = float(np.clip((self.LOW_CONC_THRESHOLD - conc_latest) * 0.5, 0.0, 100.0))
        elif conc_latest > self.HIGH_CONC_THRESHOLD:
            score = float(np.clip((conc_latest - self.HIGH_CONC_THRESHOLD) * 2.0, 0.0, 100.0))
        else:
            score = 15.0  # Moderate concentration = low systemic stress

        return {
            "score": round(score, 2),
            "country": country,
            "bank_concentration": {
                "top5_assets_pct": round(conc_latest, 2),
                "mean_pct": round(conc_mean, 2),
                "trend_pp": round(conc_trend, 2),
                "indicator": indicator_used,
                "observations": len(vals),
            },
            "concentration_level": (
                "fragmented" if conc_latest < self.LOW_CONC_THRESHOLD
                else "moderate" if conc_latest < self.HIGH_CONC_THRESHOLD
                else "oligopolistic"
            ),
        }
