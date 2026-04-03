"""Labor market scarring: long-term unemployment rate persistence post-2020.

Long-term unemployment (12+ months without work) is the primary channel
through which recessions permanently damage workers. COVID-19 disruptions
caused spikes in long-term unemployment in 2020-2021. Persistence of
elevated rates into 2022-2023 signals hysteresis: skill erosion, employer
stigma, and detachment from the labor force.

WDI indicator: SL.UEM.LTRM.ZS (long-term unemployment as % of total unemployment).
IMF research shows long-term unemployment rates above 25% correlate with
permanent employment-to-population ratio declines.

Score: low (<15%) -> STABLE, moderate (15-30%) -> WATCH,
elevated (30-50%) -> STRESS, persistent (>50%) -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class LaborMarketScarring(LayerBase):
    layer_id = "lPC"
    name = "Labor Market Scarring"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "SL.UEM.LTRM.ZS"
        name = "long-term unemployment"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for SL.UEM.LTRM.ZS",
            }

        values = [r["value"] for r in rows if r["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "all null values"}

        latest = values[0]
        trend = round(values[0] - values[-1], 3) if len(values) > 1 else None
        peak = max(values)

        # Score based on latest long-term unemployment share
        if latest < 15:
            score = 5.0 + latest * 1.0
        elif latest < 30:
            score = 20.0 + (latest - 15) * 1.5
        elif latest < 50:
            score = 42.5 + (latest - 30) * 1.25
        else:
            score = min(100.0, 67.5 + (latest - 50) * 0.65)

        # Persistence penalty: if latest is near the peak, scarring is entrenched
        if peak > 0 and (latest / peak) > 0.85:
            score = min(100.0, score + 5.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "ltu_share_pct": round(latest, 2),
                "peak_ltu_pct": round(peak, 2),
                "trend_change_pct": trend,
                "persistence_ratio": round(latest / peak, 3) if peak > 0 else None,
                "n_obs": len(values),
            },
        }
