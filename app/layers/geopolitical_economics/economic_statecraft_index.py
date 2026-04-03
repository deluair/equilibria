"""Economic Statecraft Index: trade weaponization risk via NE.TRD.GNFS.ZS + PV.EST.

Economic statecraft refers to the use of trade and financial flows as instruments
of foreign policy coercion. Countries with high trade dependence (NE.TRD.GNFS.ZS)
and low political stability (PV.EST) are most vulnerable to trade weaponization.

Methodology:
    trade_openness = latest NE.TRD.GNFS.ZS (trade % of GDP)
    trade_score = clip(trade_openness / 150.0, 0, 1)  -- 150% GDP = max reference
    pv_raw = latest PV.EST; pv_risk = 1 - ((pv_raw + 2.5) / 5.0) clamped [0,1]
    score = clip((trade_score * 0.55 + pv_risk * 0.45) * 100, 0, 100)

Score (0-100): Higher = greater economic statecraft vulnerability.

References:
    Blackwill & Harris (2016). War by Other Means. Harvard UP.
    Farrell & Newman (2019). "Weaponized Interdependence." IS 44(1).
    World Bank WDI NE.TRD.GNFS.ZS, PV.EST.
"""

from __future__ import annotations

from app.layers.base import LayerBase

_TRADE_CODE = "NE.TRD.GNFS.ZS"
_PV_CODE = "PV.EST"


class EconomicStatecraftIndex(LayerBase):
    layer_id = "lGP"
    name = "Economic Statecraft Index"

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
        trade_val = await self._fetch_latest(db, _TRADE_CODE, "trade % gdp")
        pv_val = await self._fetch_latest(db, _PV_CODE, "political stability")

        if trade_val is None and pv_val is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for NE.TRD.GNFS.ZS or PV.EST"}

        trade_score = min(max((trade_val or 0.0) / 150.0, 0.0), 1.0)
        pv_risk = 1.0 - max(0.0, min(1.0, ((pv_val or 0.0) + 2.5) / 5.0)) if pv_val is not None else 0.5

        weights = []
        if trade_val is not None:
            weights.append(trade_score * 0.55)
        if pv_val is not None:
            weights.append(pv_risk * 0.45)

        score = float(min(max(sum(weights) * 100, 0.0), 100.0))

        return {
            "score": round(score, 2),
            "trade_openness_pct_gdp": round(trade_val, 2) if trade_val is not None else None,
            "political_stability_est": round(pv_val, 4) if pv_val is not None else None,
            "trade_weaponization_score": round(trade_score, 4),
            "political_vulnerability": round(pv_risk, 4),
            "metrics": {
                "trade_indicator": _TRADE_CODE,
                "stability_indicator": _PV_CODE,
            },
        }
