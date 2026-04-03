"""Agricultural Trade Policy Distortion module.

Measures the nominal rate of assistance (NRA) to agriculture as a proxy for
trade policy distortion. High agricultural trade protection raises domestic
prices above world prices, distorting resource allocation.

Methodology:
- Query NV.AGR.TOTL.ZS (agriculture value added % GDP) as sector weight.
- Query AG.YLD.CREL.KG (cereal yield) as productivity benchmark.
- NRA proxy: deviation of ag value-added share from its long-run mean,
  adjusted for yield performance. High share with low yield -> distortion.
- Score = clip(nra_proxy * 10, 0, 100).

Sources: World Bank WDI (NV.AGR.TOTL.ZS, AG.YLD.CREL.KG)
"""

from __future__ import annotations

import statistics

from app.layers.base import LayerBase


class AgTradePolicyDistortion(LayerBase):
    layer_id = "lAP"
    name = "Agricultural Trade Policy Distortion"

    async def compute(self, db, **kwargs) -> dict:
        ag_code = "NV.AGR.TOTL.ZS"
        ag_name = "agriculture value added"
        ag_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 10",
            (ag_code, f"%{ag_name}%"),
        )

        yield_code = "AG.YLD.CREL.KG"
        yield_name = "cereal yield"
        yield_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 10",
            (yield_code, f"%{yield_name}%"),
        )

        if not ag_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no agriculture value added data"}

        ag_vals = [float(r["value"]) for r in ag_rows if r["value"] is not None]
        if not ag_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid agriculture data"}

        current_ag_share = ag_vals[0]
        mean_ag_share = statistics.mean(ag_vals)
        ag_share_deviation = abs(current_ag_share - mean_ag_share)

        yield_vals = [float(r["value"]) for r in yield_rows if r["value"] is not None]
        avg_yield = statistics.mean(yield_vals) if yield_vals else None

        # High ag share + low yield signals protected, inefficient agriculture
        distortion_proxy = ag_share_deviation * 3.0
        if avg_yield is not None and avg_yield < 3000:
            distortion_proxy += (3000 - avg_yield) / 100.0

        score = float(min(distortion_proxy * 2.0, 100.0))

        return {
            "score": round(score, 1),
            "signal": self.classify_signal(score),
            "current_ag_share_pct_gdp": round(current_ag_share, 2),
            "mean_ag_share_pct_gdp": round(mean_ag_share, 2),
            "ag_share_deviation": round(ag_share_deviation, 3),
            "avg_cereal_yield_kg_ha": round(avg_yield, 1) if avg_yield is not None else None,
            "n_obs": len(ag_vals),
            "indicators": [ag_code, yield_code],
        }
