"""Deglobalization Risk: trade openness decline trend + FDI volatility.

Deglobalization manifests as a sustained decline in trade-to-GDP ratios and
volatile or falling FDI. This module detects deglobalization exposure by
estimating the slope of NE.TRD.GNFS.ZS over recent years and measuring FDI
coefficient of variation (BX.KLT.DINV.WD.GD.ZS).

Methodology:
    trade_slope = OLS slope of trade openness over time (negative = deglobalizing)
    fdi_cov = std(fdi) / |mean(fdi)| over available observations

    decline_score = clip(-trade_slope * 5.0, 0, 100)  -- -20 ppt/yr = max
    fdi_vol_score = clip(fdi_cov * 100, 0, 100)
    score = clip(decline_score * 0.6 + fdi_vol_score * 0.4, 0, 100)

Score (0-100): Higher = greater deglobalization risk.

References:
    Antras, P. (2020). "De-globalisation? Global value chains in the
        post-COVID-19 age." NBER WP 28115.
    Baldwin, R. & Evenett, S. (2020). COVID-19 and Trade Policy. CEPR.
    World Bank WDI NE.TRD.GNFS.ZS, BX.KLT.DINV.WD.GD.ZS.
"""

from __future__ import annotations

import statistics

from app.layers.base import LayerBase

_TRADE_CODE = "NE.TRD.GNFS.ZS"
_FDI_CODE = "BX.KLT.DINV.WD.GD.ZS"


class DeglobalizationRisk(LayerBase):
    layer_id = "lGP"
    name = "Deglobalization Risk"

    async def _fetch(self, db, code: str, name: str) -> list[float]:
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        return [float(r["value"]) for r in rows if r["value"] is not None]

    @staticmethod
    def _ols_slope(vals: list[float]) -> float:
        n = len(vals)
        x = list(range(n))
        mx = sum(x) / n
        my = sum(vals) / n
        num = sum((xi - mx) * (yi - my) for xi, yi in zip(x, vals))
        den = sum((xi - mx) ** 2 for xi in x)
        return num / den if den != 0 else 0.0

    async def compute(self, db, **kwargs) -> dict:
        trade_vals = await self._fetch(db, _TRADE_CODE, "trade % gdp")
        fdi_vals = await self._fetch(db, _FDI_CODE, "fdi inflows % gdp")

        if not trade_vals and not fdi_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for NE.TRD.GNFS.ZS or BX.KLT.DINV.WD.GD.ZS"}

        trade_slope = None
        decline_score = 0.0
        if len(trade_vals) >= 3:
            trade_slope = self._ols_slope(list(reversed(trade_vals)))
            decline_score = float(min(max(-trade_slope * 5.0, 0.0), 100.0))

        fdi_cov = None
        fdi_vol_score = 0.0
        if len(fdi_vals) >= 3:
            mean_f = statistics.mean(fdi_vals)
            std_f = statistics.stdev(fdi_vals)
            fdi_cov = std_f / abs(mean_f) if mean_f != 0 else 0.0
            fdi_vol_score = float(min(fdi_cov * 100, 100.0))

        components = []
        if trade_slope is not None:
            components.append(decline_score * 0.6)
        if fdi_cov is not None:
            components.append(fdi_vol_score * 0.4)

        if not components:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient observations for trend analysis"}

        score = float(min(max(sum(components), 0.0), 100.0))

        return {
            "score": round(score, 2),
            "trade_openness_slope_ppt_yr": round(trade_slope, 4) if trade_slope is not None else None,
            "trade_decline_score": round(decline_score, 2),
            "fdi_volatility_cov": round(fdi_cov, 4) if fdi_cov is not None else None,
            "fdi_vol_score": round(fdi_vol_score, 2),
            "trade_obs": len(trade_vals),
            "fdi_obs": len(fdi_vals),
            "metrics": {
                "trade_indicator": _TRADE_CODE,
                "fdi_indicator": _FDI_CODE,
            },
        }
