"""Sanctions Economic Cost: trade openness shock via CoV of NE.TRD.GNFS.ZS + PV.EST.

Measures the economic cost of sanctions exposure by combining trade openness
volatility (coefficient of variation of trade/GDP) with political stability risk
(PV.EST, political stability and absence of violence). High trade CoV combined
with low political stability signals severe sanctions-driven trade disruption.

Methodology:
    trade_cov = std(trade_openness) / mean(trade_openness) over last 15 obs
    pv_raw = latest PV.EST (WGI, range roughly -2.5 to 2.5; normalize to 0-1)
    pv_risk = 1 - ((pv_raw + 2.5) / 5.0) clamped [0, 1]
    score = clip((trade_cov * 100 * 0.6 + pv_risk * 100 * 0.4), 0, 100)

Score (0-100): Higher = greater sanctions economic cost.

References:
    Hufbauer et al. (2007). Economic Sanctions Reconsidered. 3rd ed. PIIE.
    Felbermayr et al. (2020). "The Global Sanctions Data Base." EER 129.
    World Bank WDI NE.TRD.GNFS.ZS, PV.EST.
"""

from __future__ import annotations

import statistics

from app.layers.base import LayerBase

_TRADE_CODE = "NE.TRD.GNFS.ZS"
_PV_CODE = "PV.EST"


class SanctionsEconomicCost(LayerBase):
    layer_id = "lGP"
    name = "Sanctions Economic Cost"

    async def _fetch(self, db, code: str, name: str) -> list[float]:
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        return [float(r["value"]) for r in rows if r["value"] is not None]

    async def compute(self, db, **kwargs) -> dict:
        trade_vals = await self._fetch(db, _TRADE_CODE, "trade % gdp")
        pv_vals = await self._fetch(db, _PV_CODE, "political stability")

        if not trade_vals and not pv_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for NE.TRD.GNFS.ZS or PV.EST"}

        trade_cov = None
        if len(trade_vals) >= 3:
            mean_t = statistics.mean(trade_vals)
            std_t = statistics.stdev(trade_vals)
            trade_cov = std_t / mean_t if mean_t != 0 else 0.0

        pv_risk = None
        if pv_vals:
            pv_raw = pv_vals[0]
            pv_risk = 1.0 - max(0.0, min(1.0, (pv_raw + 2.5) / 5.0))

        components = []
        if trade_cov is not None:
            components.append(trade_cov * 100 * 0.6)
        if pv_risk is not None:
            components.append(pv_risk * 100 * 0.4)

        if not components:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data to compute score"}

        score = float(min(max(sum(components), 0.0), 100.0))

        return {
            "score": round(score, 2),
            "trade_openness_cov": round(trade_cov, 4) if trade_cov is not None else None,
            "political_stability_raw": round(pv_vals[0], 4) if pv_vals else None,
            "political_stability_risk": round(pv_risk, 4) if pv_risk is not None else None,
            "trade_obs": len(trade_vals),
            "metrics": {
                "trade_indicator": _TRADE_CODE,
                "stability_indicator": _PV_CODE,
            },
        }
