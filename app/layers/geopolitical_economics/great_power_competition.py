"""Great Power Competition: FDI concentration + trade partner HHI proxy.

Measures a country's exposure to great power competition dynamics by assessing
FDI source concentration and trade partner concentration. A country dependent on
a single major power for FDI and trade is highly exposed to great-power rivalry.

Methodology:
    BX.KLT.DINV.WD.GD.ZS = FDI net inflows (% GDP): high FDI dependence proxy
    NE.TRD.GNFS.ZS = trade openness (% GDP): trade concentration proxy

    We use CoV of FDI over time as concentration signal (volatile FDI = concentrated).
    trade_hhi_proxy = trade_openness / 200.0 clamped [0, 1] (open = more partners)
    fdi_cov_score = clip(fdi_cov * 100, 0, 100)

    score = clip(fdi_cov_score * 0.5 + (1 - trade_hhi_proxy) * 100 * 0.5, 0, 100)

    Note: Lower trade openness relative to a diversified benchmark implies fewer
    partners, i.e., higher partner concentration.

Score (0-100): Higher = greater great power competition exposure.

References:
    Mearsheimer, J. (2001). The Tragedy of Great Power Politics. Norton.
    Brands, H. (2018). "The Unexceptional Superpower." Survival 60(6).
    World Bank WDI BX.KLT.DINV.WD.GD.ZS, NE.TRD.GNFS.ZS.
"""

from __future__ import annotations

import statistics

from app.layers.base import LayerBase

_FDI_CODE = "BX.KLT.DINV.WD.GD.ZS"
_TRADE_CODE = "NE.TRD.GNFS.ZS"


class GreatPowerCompetition(LayerBase):
    layer_id = "lGP"
    name = "Great Power Competition"

    async def _fetch(self, db, code: str, name: str) -> list[float]:
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        return [float(r["value"]) for r in rows if r["value"] is not None]

    async def compute(self, db, **kwargs) -> dict:
        fdi_vals = await self._fetch(db, _FDI_CODE, "fdi inflows % gdp")
        trade_vals = await self._fetch(db, _TRADE_CODE, "trade % gdp")

        if not fdi_vals and not trade_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for BX.KLT.DINV.WD.GD.ZS or NE.TRD.GNFS.ZS"}

        fdi_cov = None
        if len(fdi_vals) >= 3:
            mean_f = statistics.mean(fdi_vals)
            std_f = statistics.stdev(fdi_vals)
            fdi_cov = std_f / abs(mean_f) if mean_f != 0 else 0.0

        trade_hhi_proxy = None
        if trade_vals:
            trade_hhi_proxy = min(max(trade_vals[0] / 200.0, 0.0), 1.0)

        components = []
        if fdi_cov is not None:
            components.append(min(fdi_cov * 100, 100.0) * 0.5)
        if trade_hhi_proxy is not None:
            components.append((1.0 - trade_hhi_proxy) * 100.0 * 0.5)

        if not components:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data to compute score"}

        score = float(min(max(sum(components), 0.0), 100.0))

        return {
            "score": round(score, 2),
            "fdi_volatility_cov": round(fdi_cov, 4) if fdi_cov is not None else None,
            "trade_openness_latest": round(trade_vals[0], 2) if trade_vals else None,
            "trade_partner_concentration_proxy": round(1.0 - trade_hhi_proxy, 4) if trade_hhi_proxy is not None else None,
            "fdi_obs": len(fdi_vals),
            "metrics": {
                "fdi_indicator": _FDI_CODE,
                "trade_indicator": _TRADE_CODE,
            },
        }
