"""Crop insurance coverage: agricultural income volatility and risk protection depth.

Methodology
-----------
**Crop insurance adequacy** proxied from two indicators:
    - NV.AGR.TOTL.ZS: Agriculture, value added (% of GDP) -- sector importance, scales
      the demand for insurance.
    - Insurance depth proxy: the coefficient of variation (CV) of agricultural value
      added across the available time series. Higher volatility implies higher insurance
      need, and if no formal insurance product data exist in the DB the gap is inferred
      from volatility alone.

    volatility_cv = std(ag_value_added) / mean(ag_value_added)

    A high-CV, high-ag-share economy with no insurance depth data is treated as a
    high-stress case (large uninsured agricultural risk).

    stress_score = (ag_share_pct / 30) * 50 + (volatility_cv * 100)

Score (0-100): higher = worse coverage (more uninsured agricultural risk).

Sources: World Bank WDI (NV.AGR.TOTL.ZS)
"""

from __future__ import annotations

import math
import statistics

from app.layers.base import LayerBase

_SQL = """
    SELECT value FROM data_points
    WHERE series_id = (
        SELECT id FROM data_series
        WHERE indicator_code = ? OR name LIKE ?
    )
    ORDER BY date DESC LIMIT 15
"""


class CropInsuranceCoverage(LayerBase):
    layer_id = "lAF"
    name = "Crop Insurance Coverage"

    async def compute(self, db, **kwargs) -> dict:
        code_ag, name_ag = "NV.AGR.TOTL.ZS", "%agriculture, value added%"

        rows_ag = await db.fetch_all(_SQL, (code_ag, name_ag))

        if not rows_ag:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no agricultural value added data"}

        ag_vals = [float(r["value"]) for r in rows_ag if r["value"] is not None]

        if not ag_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no usable agriculture data"}

        ag_mean = statistics.mean(ag_vals)
        ag_share = ag_vals[0]  # most recent

        volatility_cv = 0.0
        if len(ag_vals) >= 3:
            ag_std = statistics.stdev(ag_vals)
            volatility_cv = ag_std / ag_mean if ag_mean > 0 else 0.0

        # Sector weight component: large ag sector needs more insurance
        sector_component = min(50.0, (ag_share / 30.0) * 50.0)

        # Volatility component: high CV -> higher uninsured exposure
        volatility_component = min(50.0, volatility_cv * 100.0)

        score = min(100.0, sector_component + volatility_component)

        return {
            "score": round(score, 2),
            "metrics": {
                "ag_value_added_pct_gdp_latest": round(ag_share, 2),
                "ag_value_added_pct_gdp_mean": round(ag_mean, 2),
                "volatility_cv": round(volatility_cv, 4),
                "observations_used": len(ag_vals),
                "sector_component": round(sector_component, 2),
                "volatility_component": round(volatility_component, 2),
            },
        }
