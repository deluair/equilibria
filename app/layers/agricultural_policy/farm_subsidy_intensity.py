"""Farm Subsidy Intensity module.

Measures agricultural subsidies as a share of farm receipts (producer support
estimate proxy). High subsidy intensity can indicate policy-driven market
distortion and fiscal pressure on agricultural support programs.

Methodology:
- Query NV.AGR.TOTL.ZS (agriculture value added % GDP) as farm receipts proxy.
- Query AG.YLD.CREL.KG (cereal yield kg/ha) as productivity anchor.
- Subsidy intensity proxy: inverse of yield-adjusted ag value-add change.
- Higher score = higher distortion / subsidy dependency.

Sources: World Bank WDI (NV.AGR.TOTL.ZS, AG.YLD.CREL.KG)
"""

from __future__ import annotations

import statistics

from app.layers.base import LayerBase


class FarmSubsidyIntensity(LayerBase):
    layer_id = "lAP"
    name = "Farm Subsidy Intensity"

    async def compute(self, db, **kwargs) -> dict:
        code = "NV.AGR.TOTL.ZS"
        name = "agriculture value added"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 10",
            (code, f"%{name}%"),
        )

        yield_code = "AG.YLD.CREL.KG"
        yield_name = "cereal yield"
        yield_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 10",
            (yield_code, f"%{yield_name}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no agriculture value added data"}

        values = [float(r["value"]) for r in rows if r["value"] is not None]
        if len(values) < 2:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data points"}

        ag_share = statistics.mean(values)
        ag_volatility = statistics.stdev(values) if len(values) > 1 else 0.0

        yield_vals = [float(r["value"]) for r in yield_rows if r["value"] is not None]
        avg_yield = statistics.mean(yield_vals) if yield_vals else None

        # High ag share + high volatility -> higher subsidy dependency score
        # Ag share >15% with high volatility signals subsidy-propped sectors
        base_score = min(ag_share * 2.5, 60.0)
        vol_penalty = min(ag_volatility * 5.0, 40.0)
        score = float(min(base_score + vol_penalty, 100.0))

        return {
            "score": round(score, 1),
            "signal": self.classify_signal(score),
            "ag_value_added_pct_gdp": round(ag_share, 2),
            "ag_share_volatility": round(ag_volatility, 3),
            "avg_cereal_yield_kg_ha": round(avg_yield, 1) if avg_yield is not None else None,
            "n_obs": len(values),
            "indicators": [code, yield_code],
        }
