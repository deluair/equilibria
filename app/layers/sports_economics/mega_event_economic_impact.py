"""Mega-event economic impact: tourism revenue surge around major sporting events.

International tourism receipts (WDI BX.TRF.TRVL.CD) capture the demand-side
shock from hosting large events such as the Olympic Games, FIFA World Cup, and
continental championships. A steep multi-year uptrend in receipts relative to
the preceding base period signals genuine event-driven multiplier effects.

Score: measured by year-on-year growth rate in tourism receipts. Negative or
flat growth -> STABLE baseline; moderate growth (0-10%) -> WATCH; strong
growth (10-25%) -> STRESS (crowding pressure); exceptional growth (>25%)
-> CRISIS (infrastructure overload, price volatility).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class MegaEventEconomicImpact(LayerBase):
    layer_id = "lSP"
    name = "Mega-Event Economic Impact"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "BX.TRF.TRVL.CD"
        name = "tourism receipts"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        values = [r["value"] for r in rows if r["value"] is not None]
        if not values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for BX.TRF.TRVL.CD",
            }

        latest = values[0]
        if len(values) < 2:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient observations for growth calculation",
            }

        prior = values[1]
        yoy_growth_pct = ((latest - prior) / prior * 100.0) if prior != 0 else 0.0

        if yoy_growth_pct <= 0:
            score = max(0.0, 20.0 + yoy_growth_pct * 0.5)
        elif yoy_growth_pct < 10:
            score = 20.0 + yoy_growth_pct * 3.0
        elif yoy_growth_pct < 25:
            score = 50.0 + (yoy_growth_pct - 10) * 1.67
        else:
            score = min(100.0, 75.0 + (yoy_growth_pct - 25) * 1.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "tourism_receipts_usd": round(latest, 0),
                "yoy_growth_pct": round(yoy_growth_pct, 2),
                "n_obs": len(values),
                "impact_level": (
                    "baseline" if yoy_growth_pct <= 0
                    else "moderate" if yoy_growth_pct < 10
                    else "strong" if yoy_growth_pct < 25
                    else "exceptional"
                ),
            },
        }
