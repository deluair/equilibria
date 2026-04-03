"""Carbon lock-in index: NE.GDI.FTOT.ZS + EN.ATM.CO2E.KT.

Methodology
-----------
Carbon lock-in occurs when past investment decisions entrench fossil fuel
infrastructure, making future decarbonization costly. It is measured here
using two complementary signals:

1. Foreign direct investment net inflows (NE.GDI.FTOT.ZS, % of GDP): used as a
   proxy for capital attracted to high-carbon activities. In fossil-dependent
   economies, FDI often flows into extractive industries.
2. CO2 emissions level and trend (EN.ATM.CO2E.KT): high absolute emissions
   combined with slow decline indicate embedded carbon infrastructure.

The lock-in index combines:
- Emissions intensity per unit of FDI inflow (higher = more carbon per
  unit of capital, suggesting lock-in)
- Trend of emissions over recent years

Score: 0 = rapidly decarbonizing with low FDI-to-emissions ratio, 100 = rising
emissions and high carbon-intensive capital flows.

Sources: World Bank WDI NE.GDI.FTOT.ZS (FDI net inflows, % GDP),
EN.ATM.CO2E.KT (CO2 emissions, kt).
IEA World Energy Investment 2023. UNEP Production Gap Report 2023.
"""

from __future__ import annotations

import math

import numpy as np

from app.layers.base import LayerBase

_CO2_CODE = "EN.ATM.CO2E.KT"
_CO2_NAME = "CO2 emissions"
_FDI_CODE = "NE.GDI.FTOT.ZS"
_FDI_NAME = "Foreign direct investment"


class CarbonLockInIndex(LayerBase):
    layer_id = "lGT"
    name = "Carbon Lock-In Index"

    async def compute(self, db, **kwargs) -> dict:
        co2_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (_CO2_CODE, f"%{_CO2_NAME}%"),
        )
        fdi_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (_FDI_CODE, f"%{_FDI_NAME}%"),
        )

        if not co2_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no CO2 emissions data (EN.ATM.CO2E.KT)"}

        co2_vals = [float(r["value"]) for r in co2_rows if r["value"] is not None and float(r["value"]) > 0]
        if len(co2_vals) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient CO2 data points"}

        # Emissions trend sub-score (0-60): rising emissions = higher score
        log_vals = np.array([math.log(v) for v in co2_vals], dtype=float)
        t = np.arange(len(log_vals), dtype=float)
        slope = float(np.polyfit(t[::-1], log_vals, 1)[0])
        annual_pct_change = slope * 100  # positive = growing

        if annual_pct_change >= 5.0:
            trend_score = 60.0
        elif annual_pct_change >= 0:
            trend_score = annual_pct_change / 5.0 * 60
        elif annual_pct_change >= -7.0:
            # falling: 60 * (1 - |rate|/7)
            trend_score = max(0.0, 60 * (1 + annual_pct_change / 7.0))
        else:
            trend_score = 0.0

        # FDI intensity sub-score (0-40): proxy for carbon-intensive capital
        fdi_vals = [float(r["value"]) for r in fdi_rows if r["value"] is not None] if fdi_rows else []
        latest_fdi = fdi_vals[0] if fdi_vals else None

        # High FDI into high-CO2 economy = higher lock-in risk
        latest_co2 = co2_vals[0]
        if latest_fdi is None:
            fdi_score = 20.0  # neutral
        elif latest_fdi <= 0:
            fdi_score = 0.0
        else:
            # FDI > 5% GDP in a high-emission economy signals lock-in
            fdi_score = min(latest_fdi / 5.0 * 20, 40.0)
            # Scale by emissions level (normalised to 500,000 kt as rough median)
            co2_scale = min(latest_co2 / 500_000, 2.0)
            fdi_score = min(fdi_score * co2_scale, 40.0)

        score = min(trend_score + fdi_score, 100.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "co2_annual_pct_change": round(annual_pct_change, 3),
                "latest_co2_kt": round(latest_co2, 0),
                "fdi_net_inflows_pct_gdp": round(latest_fdi, 3) if latest_fdi is not None else None,
                "co2_observations": len(co2_vals),
            },
        }
