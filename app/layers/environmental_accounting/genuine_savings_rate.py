"""Genuine savings rate: adjusted net savings including natural capital depletion and pollution damage.

Genuine savings (adjusted net savings) = gross national savings - fixed capital depreciation
+ education expenditure - natural resource depletion - CO2 damage - PM2.5 damage.

A negative genuine savings rate signals that a country is running down its total wealth
(produced + human + natural capital) and is on an unsustainable path.

Methodology:
    ANS = GNS - Dh + Edu - RD - CO2D - PM25D
    where:
        GNS  = gross national savings (% GNI)
        Dh   = consumption of fixed capital (% GNI)
        Edu  = education expenditure (% GNI)
        RD   = total natural resource depletion (% GNI)
        CO2D = CO2 damage (% GNI)
        PM25D = PM2.5 pollution damage (% GNI)

References:
    Hamilton, K. & Clemens, M. (1999). "Genuine savings rates in developing countries."
        World Bank Economic Review, 13(2), 333-356.
    World Bank (2021). "The Changing Wealth of Nations 2021."
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class GenuineSavingsRate(LayerBase):
    layer_id = "lEA"
    name = "Genuine Savings Rate"

    async def compute(self, db, **kwargs) -> dict:
        code = "NY.ADJ.SVNX.GN.ZS"
        name = "adjusted net savings"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no genuine savings data"}

        values = [float(r["value"]) for r in rows if r["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid genuine savings values"}

        latest = values[0]
        avg = float(np.mean(values))
        trend_slope = None
        if len(values) >= 3:
            xs = np.arange(len(values), dtype=float)
            trend_slope = float(np.polyfit(xs, values, 1)[0])

        # Score: negative ANS is bad (high stress). Map: ANS >= 15 -> 10, ANS <= -10 -> 90.
        score = float(np.clip(50.0 - latest * 2.5, 10.0, 90.0))

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "indicator_code": code,
                "latest_ans_pct_gni": round(latest, 2),
                "mean_ans_pct_gni": round(avg, 2),
                "trend_slope": round(trend_slope, 4) if trend_slope is not None else None,
                "sustainable": latest > 0,
                "n_obs": len(values),
            },
        }
