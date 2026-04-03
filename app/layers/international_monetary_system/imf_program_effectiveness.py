"""IMF program effectiveness: GDP recovery rate post-IMF program.

IMF structural adjustment programs aim to restore macroeconomic stability,
but their effectiveness is contested (Przeworski & Vreeland, 2000; Bird, 2001).
This module proxies effectiveness via GDP growth volatility: countries with
recurrent IMF programs exhibit persistent growth instability, suggesting
limited program durability.

WDI NY.GDP.MKTP.KD.ZG provides annual GDP growth. High volatility over
rolling windows proxies the boom-bust cycles that accompany crisis lending.

Score: stable positive growth -> STABLE; high volatility with recurrent
negative growth -> STRESS/CRISIS.
"""

from __future__ import annotations

import statistics

from app.layers.base import LayerBase


class IMFProgramEffectiveness(LayerBase):
    layer_id = "lMS"
    name = "IMF Program Effectiveness"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "NY.GDP.MKTP.KD.ZG"
        name = "GDP growth"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 20",
            (code, f"%{name}%"),
        )

        vals = [r["value"] for r in rows if r["value"] is not None]
        if not vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for NY.GDP.MKTP.KD.ZG",
            }

        latest = vals[0]
        vol = round(statistics.stdev(vals), 3) if len(vals) > 2 else abs(latest)
        avg_growth = round(statistics.mean(vals), 3)
        neg_episodes = sum(1 for v in vals if v < 0)

        # Score: high volatility + recurrent recessions = low effectiveness
        base_vol_score = min(70.0, vol * 5.0)

        # Negative growth penalty
        if latest < -3:
            recession_penalty = 25.0
        elif latest < 0:
            recession_penalty = 15.0
        elif neg_episodes > 3:
            recession_penalty = 10.0
        else:
            recession_penalty = 0.0

        score = round(min(100.0, base_vol_score + recession_penalty), 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "latest_gdp_growth_pct": round(latest, 2),
                "avg_gdp_growth_pct": avg_growth,
                "growth_volatility": vol,
                "negative_growth_episodes": neg_episodes,
                "n_obs": len(vals),
                "recovery_quality": "strong" if avg_growth > 3 and vol < 3 else "weak",
            },
        }
