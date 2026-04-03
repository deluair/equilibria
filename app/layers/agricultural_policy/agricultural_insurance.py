"""Agricultural Insurance module.

Measures crop insurance penetration rate as a share of agricultural output.
Low insurance coverage leaves farmers exposed to weather and price shocks,
increasing food security vulnerability.

Methodology:
- Query NV.AGR.TOTL.ZS (agriculture value added % GDP) as sector exposure proxy.
- Query FP.CPI.TOTL.ZG (CPI inflation) as macro risk environment.
- Insurance penetration proxy: computed from climate and ag sector volatility.
  High ag GDP share + high inflation volatility without coverage -> higher risk gap.
- Score = clip((ag_share * cpi_vol_factor) / benchmark, 0, 100).

Sources: World Bank WDI (NV.AGR.TOTL.ZS, FP.CPI.TOTL.ZG)
"""

from __future__ import annotations

import statistics

from app.layers.base import LayerBase


class AgriculturalInsurance(LayerBase):
    layer_id = "lAP"
    name = "Agricultural Insurance"

    async def compute(self, db, **kwargs) -> dict:
        ag_code = "NV.AGR.TOTL.ZS"
        ag_name = "agriculture value added"
        ag_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 10",
            (ag_code, f"%{ag_name}%"),
        )

        cpi_code = "FP.CPI.TOTL.ZG"
        cpi_name = "CPI inflation"
        cpi_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 10",
            (cpi_code, f"%{cpi_name}%"),
        )

        if not ag_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no agriculture value added data"}

        ag_vals = [float(r["value"]) for r in ag_rows if r["value"] is not None]
        if not ag_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid ag data"}

        avg_ag_share = statistics.mean(ag_vals)
        ag_vol = statistics.stdev(ag_vals) if len(ag_vals) > 1 else 0.0

        cpi_vals = [float(r["value"]) for r in cpi_rows if r["value"] is not None]
        cpi_vol = statistics.stdev(cpi_vals) if len(cpi_vals) > 1 else 0.0
        avg_cpi = statistics.mean(cpi_vals) if cpi_vals else None

        # Risk exposure = ag sector size * macro volatility environment
        # Higher score = greater insurance gap (more unprotected exposure)
        risk_exposure = avg_ag_share * (1.0 + cpi_vol / 10.0)
        # Normalize: benchmark is 5% ag share with stable prices -> low risk
        score = float(min((risk_exposure / 5.0) * 20.0, 100.0))

        return {
            "score": round(score, 1),
            "signal": self.classify_signal(score),
            "avg_ag_value_added_pct_gdp": round(avg_ag_share, 2),
            "ag_volatility": round(ag_vol, 3),
            "cpi_volatility_std": round(cpi_vol, 3),
            "avg_cpi_inflation_pct": round(avg_cpi, 2) if avg_cpi is not None else None,
            "risk_exposure_index": round(risk_exposure, 3),
            "n_obs_ag": len(ag_vals),
            "n_obs_cpi": len(cpi_vals) if cpi_vals else 0,
            "indicators": [ag_code, cpi_code],
        }
