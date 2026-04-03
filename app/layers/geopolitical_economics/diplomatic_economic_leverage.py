"""Diplomatic Economic Leverage: governance quality + trade openness composite.

Measures a country's diplomatic economic leverage -- the degree to which it can
deploy or withstand economic pressure in diplomatic contexts. Strong rule of law
(RL.EST), voice and accountability (VA.EST), and high trade integration
(NE.TRD.GNFS.ZS) signal leverage through institutional credibility and market
access. Low values indicate vulnerability to coercion.

Methodology:
    rl_norm = (rl_raw + 2.5) / 5.0 clamped [0, 1]
    va_norm = (va_raw + 2.5) / 5.0 clamped [0, 1]
    trade_norm = clip(trade_openness / 150.0, 0, 1)

    leverage_index = rl_norm * 0.35 + va_norm * 0.35 + trade_norm * 0.30
    score = clip((1 - leverage_index) * 100, 0, 100)

    Score inverted: high leverage = low vulnerability score.

Score (0-100): Higher = lower diplomatic economic leverage (higher vulnerability).

References:
    Keohane, R. & Nye, J. (1977). Power and Interdependence. Little, Brown.
    Drezner, D. (2008). "All Politics Is Global." Princeton UP.
    World Bank WDI RL.EST, VA.EST, NE.TRD.GNFS.ZS.
"""

from __future__ import annotations

from app.layers.base import LayerBase

_RL_CODE = "RL.EST"
_VA_CODE = "VA.EST"
_TRADE_CODE = "NE.TRD.GNFS.ZS"


class DiplomaticEconomicLeverage(LayerBase):
    layer_id = "lGP"
    name = "Diplomatic Economic Leverage"

    async def _fetch_latest(self, db, code: str, name: str) -> float | None:
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        for r in rows:
            if r["value"] is not None:
                return float(r["value"])
        return None

    async def compute(self, db, **kwargs) -> dict:
        rl_raw = await self._fetch_latest(db, _RL_CODE, "rule of law")
        va_raw = await self._fetch_latest(db, _VA_CODE, "voice accountability")
        trade_open = await self._fetch_latest(db, _TRADE_CODE, "trade % gdp")

        if rl_raw is None and va_raw is None and trade_open is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for RL.EST, VA.EST, or NE.TRD.GNFS.ZS"}

        rl_norm = max(0.0, min(1.0, ((rl_raw or 0.0) + 2.5) / 5.0))
        va_norm = max(0.0, min(1.0, ((va_raw or 0.0) + 2.5) / 5.0))
        trade_norm = min(max((trade_open or 0.0) / 150.0, 0.0), 1.0)

        weights_sum = 0.0
        leverage_index = 0.0
        if rl_raw is not None:
            leverage_index += rl_norm * 0.35
            weights_sum += 0.35
        if va_raw is not None:
            leverage_index += va_norm * 0.35
            weights_sum += 0.35
        if trade_open is not None:
            leverage_index += trade_norm * 0.30
            weights_sum += 0.30

        leverage_normalized = leverage_index / weights_sum if weights_sum > 0 else 0.5
        score = float(min(max((1.0 - leverage_normalized) * 100, 0.0), 100.0))

        return {
            "score": round(score, 2),
            "rule_of_law_est": round(rl_raw, 4) if rl_raw is not None else None,
            "voice_accountability_est": round(va_raw, 4) if va_raw is not None else None,
            "trade_openness_pct_gdp": round(trade_open, 2) if trade_open is not None else None,
            "leverage_index": round(leverage_normalized, 4),
            "interpretation": "lower score = stronger leverage; higher score = greater vulnerability",
            "metrics": {
                "rule_of_law_indicator": _RL_CODE,
                "voice_accountability_indicator": _VA_CODE,
                "trade_indicator": _TRADE_CODE,
            },
        }
