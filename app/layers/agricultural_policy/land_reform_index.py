"""Land Reform Index module.

Measures land inequality as a proxy for land reform effectiveness.
Uses agricultural employment share and land use patterns to construct
a land distribution inequality proxy (Gini-adjacent).

Methodology:
- Query SL.AGR.EMPL.ZS (agricultural employment % total employment).
- Query NV.AGR.TOTL.ZS (agriculture value added % GDP).
- Land inequality proxy: ratio of ag employment share to ag value-add share.
  High ratio (many workers produce little value) suggests concentrated land ownership.
- Proxy Gini = 1 - (ag_value_share / ag_empl_share), clamped [0,1].
- Score = proxy_gini * 100.

Sources: World Bank WDI (SL.AGR.EMPL.ZS, NV.AGR.TOTL.ZS)
"""

from __future__ import annotations

import statistics

from app.layers.base import LayerBase


class LandReformIndex(LayerBase):
    layer_id = "lAP"
    name = "Land Reform Index"

    async def compute(self, db, **kwargs) -> dict:
        empl_code = "SL.AGR.EMPL.ZS"
        empl_name = "employment in agriculture"
        empl_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 10",
            (empl_code, f"%{empl_name}%"),
        )

        ag_code = "NV.AGR.TOTL.ZS"
        ag_name = "agriculture value added"
        ag_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 10",
            (ag_code, f"%{ag_name}%"),
        )

        if not empl_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no agricultural employment data"}
        if not ag_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no agriculture value added data"}

        empl_vals = [float(r["value"]) for r in empl_rows if r["value"] is not None]
        ag_vals = [float(r["value"]) for r in ag_rows if r["value"] is not None]

        if not empl_vals or not ag_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid data"}

        avg_empl = statistics.mean(empl_vals)
        avg_ag_share = statistics.mean(ag_vals)

        if avg_empl < 1e-6:
            return {"score": None, "signal": "UNAVAILABLE", "error": "zero agricultural employment"}

        # Land inequality proxy: labor/value ratio
        # If many workers produce little ag GDP share -> unequal land distribution
        labor_value_ratio = avg_empl / (avg_ag_share + 1e-6)
        proxy_gini = float(min(max(1.0 - (1.0 / labor_value_ratio), 0.0), 1.0))
        score = round(proxy_gini * 100.0, 1)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "avg_ag_employment_pct": round(avg_empl, 2),
            "avg_ag_value_added_pct_gdp": round(avg_ag_share, 2),
            "labor_value_ratio": round(labor_value_ratio, 3),
            "land_gini_proxy": round(proxy_gini, 4),
            "n_obs_empl": len(empl_vals),
            "n_obs_ag": len(ag_vals),
            "indicators": [empl_code, ag_code],
        }
