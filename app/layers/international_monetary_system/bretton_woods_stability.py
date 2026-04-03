"""Bretton Woods stability: exchange rate regime stability composite.

The post-Bretton Woods system (post-1973) is characterized by floating
exchange rates with periodic instability. This module proxies exchange rate
regime stability via the official exchange rate (LCU per USD) and its
year-over-year volatility (WDI PA.NUS.FCRF). High volatility signals regime
stress and potential balance-of-payments crisis.

Mundell's impossible trinity: no country can simultaneously maintain a fixed
exchange rate, free capital movement, and independent monetary policy.
Exchange rate volatility reflects trilemma trade-off adjustments.

Score: low volatility -> STABLE (managed/peg credible or orderly float);
high volatility -> STRESS/CRISIS.
"""

from __future__ import annotations

import statistics

from app.layers.base import LayerBase


class BrettonWoodsStability(LayerBase):
    layer_id = "lMS"
    name = "Bretton Woods Stability"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "PA.NUS.FCRF"
        name = "Official exchange rate"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 20",
            (code, f"%{name}%"),
        )

        vals = [r["value"] for r in rows if r["value"] is not None]
        if not vals or len(vals) < 2:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data for PA.NUS.FCRF",
            }

        # Compute year-over-year percent changes as volatility proxy
        yoy_changes = []
        for i in range(len(vals) - 1):
            if vals[i + 1] and vals[i + 1] != 0:
                yoy_changes.append(abs((vals[i] - vals[i + 1]) / vals[i + 1]) * 100)

        if not yoy_changes:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "cannot compute exchange rate change series",
            }

        avg_vol = round(statistics.mean(yoy_changes), 3)
        latest_change = yoy_changes[0] if yoy_changes else None

        # Score: higher volatility = more stress
        if avg_vol < 2:
            score = 8.0
        elif avg_vol < 5:
            score = 8.0 + (avg_vol - 2) * 4.0
        elif avg_vol < 10:
            score = 20.0 + (avg_vol - 5) * 4.0
        elif avg_vol < 20:
            score = 40.0 + (avg_vol - 10) * 3.0
        elif avg_vol < 40:
            score = 70.0 + (avg_vol - 20) * 1.0
        else:
            score = min(100.0, 90.0 + (avg_vol - 40) * 0.5)

        score = round(score, 2)
        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "avg_annual_fx_change_pct": avg_vol,
                "latest_yoy_change_pct": round(latest_change, 2) if latest_change is not None else None,
                "n_obs": len(vals),
                "n_changes": len(yoy_changes),
                "regime_stress": avg_vol > 10,
            },
        }
