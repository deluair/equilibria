"""Energy transition investment: EG.ELC.RNEW.ZS + NE.GDI.TOTL.ZS proxy.

Methodology
-----------
Energy transition investment captures whether a country is allocating sufficient
capital to shift its energy system. Direct green investment data is not available
in WDI; this module proxies it through:

1. Renewable electricity share trend (EG.ELC.RNEW.ZS growth): sustained growth
   implies ongoing investment in renewable capacity.
2. Gross fixed capital formation rate (NE.GDI.TOTL.ZS): overall investment
   intensity provides the capital base for transition spending.

IEA Net Zero requires global clean energy investment to reach $4 trillion/yr by
2030 (from ~$1.7 trillion in 2021). At country level, the proxy captures whether
the investment environment is consistent with transition-scale capital deployment.

Scoring framework:
- Renewable growth >= 3 pp/yr AND investment >= 25% GDP: strong transition signal
- Partial conditions: proportional scoring
- Falling renewable share or very low investment: high score (stress)

Score: 0 = strong transition investment signal, 100 = weak/absent signal.

Sources: World Bank WDI EG.ELC.RNEW.ZS, NE.GDI.TOTL.ZS.
IEA World Energy Outlook 2023. BNEF Energy Transition Investment Trends 2023.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_REN_CODE = "EG.ELC.RNEW.ZS"
_REN_NAME = "Renewable electricity output"
_INV_CODE = "NE.GDI.TOTL.ZS"
_INV_NAME = "Gross fixed capital formation"


class EnergyTransitionInvestment(LayerBase):
    layer_id = "lGT"
    name = "Energy Transition Investment"

    REN_GROWTH_TARGET = 3.0   # pp/yr (IEA NZE consistent for developing economies)
    INVEST_TARGET = 25.0      # % of GDP

    async def compute(self, db, **kwargs) -> dict:
        ren_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (_REN_CODE, f"%{_REN_NAME}%"),
        )
        inv_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (_INV_CODE, f"%{_INV_NAME}%"),
        )

        if not ren_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no renewable electricity data (EG.ELC.RNEW.ZS)"}

        ren_vals = [float(r["value"]) for r in ren_rows if r["value"] is not None]
        if len(ren_vals) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient renewable electricity data points"}

        arr = np.array(ren_vals, dtype=float)
        t = np.arange(len(arr), dtype=float)
        ren_growth = float(np.polyfit(t[::-1], arr, 1)[0])  # pp/yr

        # Renewable growth sub-score (0-60)
        if ren_growth >= self.REN_GROWTH_TARGET:
            ren_score = 0.0
        elif ren_growth >= 0:
            ren_score = (self.REN_GROWTH_TARGET - ren_growth) / self.REN_GROWTH_TARGET * 50
        else:
            # declining: 50-80 range
            ren_score = min(50 + (-ren_growth / self.REN_GROWTH_TARGET) * 30, 80.0)

        # Investment rate sub-score (0-40)
        inv_vals = [float(r["value"]) for r in inv_rows if r["value"] is not None] if inv_rows else []
        latest_inv = inv_vals[0] if inv_vals else None

        if latest_inv is None:
            inv_score = 20.0  # neutral
        elif latest_inv >= self.INVEST_TARGET:
            inv_score = 0.0
        else:
            inv_score = (self.INVEST_TARGET - latest_inv) / self.INVEST_TARGET * 40

        score = min(ren_score + inv_score, 100.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "renewable_growth_pp_yr": round(ren_growth, 3),
                "gross_fixed_capital_formation_pct_gdp": round(latest_inv, 2) if latest_inv is not None else None,
                "ren_growth_target_pp_yr": self.REN_GROWTH_TARGET,
                "invest_target_pct_gdp": self.INVEST_TARGET,
                "observations": len(ren_vals),
            },
        }
