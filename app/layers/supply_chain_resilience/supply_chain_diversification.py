"""Supply chain diversification: trade-to-GDP as diversification proxy.

Uses NE.TRD.GNFS.ZS (trade as % of GDP) which captures both export and import
integration. High trade openness combined with stable ratios signals diversified
supply chains; excessive concentration in trade signals fragility.

Methodology:
    Fetch up to 15 observations of NE.TRD.GNFS.ZS. High but stable trade openness
    scores lower (more diversified). Score penalizes extremes:
        score = clip(abs(trade_share - 80) * 0.8, 0, 100)

    At 80% trade/GDP: score = 0 (balanced diversification reference).
    At 0% or 205%: score = 100 (extreme isolation or over-dependence).

Score (0-100): Higher score indicates less diversified supply chain exposure.

References:
    World Bank WDI NE.TRD.GNFS.ZS.
    Farole & Winkler (2014). "Making Foreign Direct Investment Work for Sub-Saharan Africa."
    WTO (2021). "Global Value Chain Development Report."
"""

from __future__ import annotations

import statistics

from app.layers.base import LayerBase

_CODE = "NE.TRD.GNFS.ZS"
_NAME = "trade"


class SupplyChainDiversification(LayerBase):
    layer_id = "lSR"
    name = "Supply Chain Diversification"

    async def compute(self, db, **kwargs) -> dict:
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (_CODE, f"%{_NAME}%"),
        )

        values = [float(r["value"]) for r in rows if r["value"] is not None]

        if not values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"no data for {_CODE} (trade openness proxy)",
            }

        mean_trade_share = statistics.mean(values)
        score = float(min(max(abs(mean_trade_share - 80.0) * 0.8, 0.0), 100.0))
        volatility = round(statistics.stdev(values), 2) if len(values) > 1 else None

        return {
            "score": round(score, 2),
            "mean_trade_share_pct_gdp": round(mean_trade_share, 2),
            "trade_share_volatility": volatility,
            "n_obs": len(values),
            "indicator": _CODE,
        }
