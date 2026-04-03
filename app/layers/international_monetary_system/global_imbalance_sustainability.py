"""Global imbalance sustainability: current account surplus/deficit spread.

Persistent current account imbalances across major economies pose systemic
risk to the international monetary system. The IMF's External Sector Report
identifies excessive imbalances as a core vulnerability. This module uses
WDI BN.CAB.XOKA.GD.ZS (current account balance as % of GDP) to assess
imbalance magnitude and trend.

Feldstein-Horioka (1980) puzzle: high saving-investment correlations suggest
limited capital mobility; large CA deficits proxy for high reliance on foreign
financing.

Score: balanced CA (near zero) -> STABLE; large persistent deficit or surplus
-> STRESS/CRISIS.
"""

from __future__ import annotations

import statistics

from app.layers.base import LayerBase


class GlobalImbalanceSustainability(LayerBase):
    layer_id = "lMS"
    name = "Global Imbalance Sustainability"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "BN.CAB.XOKA.GD.ZS"
        name = "current account balance"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        vals = [r["value"] for r in rows if r["value"] is not None]
        if not vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for BN.CAB.XOKA.GD.ZS",
            }

        latest = vals[0]
        abs_latest = abs(latest)
        trend = round(vals[0] - vals[-1], 3) if len(vals) > 1 else None
        vol = round(statistics.stdev(vals), 3) if len(vals) > 2 else None

        # Score: magnitude of imbalance (both surplus and deficit are risky)
        if abs_latest < 2:
            score = 10.0
        elif abs_latest < 4:
            score = 10.0 + (abs_latest - 2) * 7.5
        elif abs_latest < 6:
            score = 25.0 + (abs_latest - 4) * 10.0
        elif abs_latest < 10:
            score = 45.0 + (abs_latest - 6) * 7.5
        else:
            score = min(100.0, 75.0 + (abs_latest - 10) * 2.5)

        # Volatility premium
        if vol is not None and vol > 3:
            score = min(100.0, score + 5.0)

        score = round(score, 2)
        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "ca_balance_gdp_pct": round(latest, 2),
                "abs_imbalance_pct": round(abs_latest, 2),
                "trend_change": trend,
                "volatility": vol,
                "n_obs": len(vals),
                "imbalance_type": "surplus" if latest > 0 else "deficit" if latest < 0 else "balanced",
            },
        }
