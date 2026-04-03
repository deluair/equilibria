"""Green jobs creation proxy: EG.ELC.RNEW.ZS + NE.GDI.TOTL.ZS.

Methodology
-----------
A direct green jobs indicator is not available in WDI. This module constructs a
proxy by combining two signals:

1. Renewable electricity share (EG.ELC.RNEW.ZS): a higher and growing share
   implies investment in renewable energy and associated job creation.
2. Gross fixed capital formation as % of GDP (NE.GDI.TOTL.ZS): sustained
   investment intensity is a precondition for green job growth.

The composite proxy score rewards:
- Renewable electricity share >= 50% and growing at >= 2 pp/yr
- Investment rate >= 25% of GDP

ILO/IRENA World Energy and Jobs reports estimate global renewable energy jobs
reached 13.7 million in 2022, dominated by solar PV and wind; countries with
>30% renewable share and strong capital formation tend to lead.

Score: 0 = strong green job proxy conditions, 100 = weak (low renewables, low investment).

Sources: World Bank WDI EG.ELC.RNEW.ZS, NE.GDI.TOTL.ZS.
IRENA Renewable Energy and Jobs Annual Review 2023.
ILO World Employment and Social Outlook 2023.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_REN_CODE = "EG.ELC.RNEW.ZS"
_REN_NAME = "Renewable electricity output"
_INV_CODE = "NE.GDI.TOTL.ZS"
_INV_NAME = "Gross fixed capital formation"


class GreenJobsCreation(LayerBase):
    layer_id = "lGT"
    name = "Green Jobs Creation"

    # Thresholds derived from IEA/IRENA benchmarks
    RENEW_SHARE_TARGET = 50.0  # % electricity from renewables
    RENEW_GROWTH_TARGET = 2.0  # pp/yr minimum growth
    INVEST_TARGET = 25.0       # % of GDP gross fixed capital formation

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

        latest_ren = ren_vals[0]
        arr = np.array(ren_vals, dtype=float)
        t = np.arange(len(arr), dtype=float)
        ren_growth = float(np.polyfit(t[::-1], arr, 1)[0])  # pp/yr

        # Renewable share sub-score (0-60)
        if latest_ren >= self.RENEW_SHARE_TARGET and ren_growth >= self.RENEW_GROWTH_TARGET:
            ren_score = 0.0
        elif latest_ren >= self.RENEW_SHARE_TARGET:
            ren_score = 10.0
        elif latest_ren >= 30.0:
            ren_score = 30.0 + (self.RENEW_SHARE_TARGET - latest_ren) / self.RENEW_SHARE_TARGET * 20
        else:
            ren_score = min(60.0, 50.0 + (30.0 - latest_ren) / 30.0 * 10)

        # Investment sub-score (0-40)
        inv_vals = [float(r["value"]) for r in inv_rows if r["value"] is not None] if inv_rows else []
        latest_inv = inv_vals[0] if inv_vals else None
        if latest_inv is None:
            inv_score = 20.0  # neutral when unavailable
        elif latest_inv >= self.INVEST_TARGET:
            inv_score = 0.0
        else:
            inv_score = (self.INVEST_TARGET - latest_inv) / self.INVEST_TARGET * 40

        score = min(ren_score + inv_score, 100.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "renewable_electricity_share_pct": round(latest_ren, 2),
                "renewable_growth_pp_yr": round(ren_growth, 3),
                "gross_fixed_capital_formation_pct_gdp": round(latest_inv, 2) if latest_inv is not None else None,
                "renew_share_target_pct": self.RENEW_SHARE_TARGET,
                "invest_target_pct_gdp": self.INVEST_TARGET,
            },
        }
