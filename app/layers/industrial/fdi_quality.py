"""FDI quality: inflows as share of GDP, trend, and manufacturing linkage.

Foreign direct investment (FDI) quality distinguishes between footloose
capital seeking cheap labor or tax arbitrage (low quality) and market-seeking
or efficiency-seeking FDI that transfers technology, creates backward linkages,
and upgrades domestic industrial capacity (Alfaro et al. 2009).

FDI net inflows as a % of GDP (BX.KLT.DINV.WD.GD.ZS) captures the intensity
of foreign investment. Trend analysis reveals whether the economy is becoming
more or less attractive. A declining FDI share with stagnant manufacturing
is a dual stress signal: foreign capital is exiting while domestic industry
fails to compensate.

Quality heuristic: FDI that accompanies rising manufacturing value added is
productive (linkage effect). FDI with stagnant or declining manufacturing
may reflect enclave investment (extractive, minimal spillovers).

Score construction:
    level_score = max(0, 10 - fdi_pct) * 8   [low FDI = stress; >10% = 0 stress from level]
    trend_adjustment = -slope * 200           [declining FDI adds stress]
    linkage_bonus = -20 if manf also declining else 0  [double stress if both declining]
    score = clip(level_score + trend_adjustment + linkage_bonus, 0, 100)

References:
    Alfaro, L. et al. (2009). FDI, productivity and financial development.
        World Economy 32(1): 111-135.
    UNCTAD (2023). World Investment Report. New York: UN.
    World Bank WDI: BX.KLT.DINV.WD.GD.ZS, NV.IND.MANF.ZS.

Indicator: BX.KLT.DINV.WD.GD.ZS (FDI net inflows, % of GDP).
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class FDIQuality(LayerBase):
    layer_id = "l14"
    name = "FDI Quality"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        fdi_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.series_id = 'BX.KLT.DINV.WD.GD.ZS'
              AND ds.country_iso3 = ?
              AND dp.value IS NOT NULL
            ORDER BY dp.date ASC
            """,
            (country,),
        )

        manf_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.series_id = 'NV.IND.MANF.ZS'
              AND ds.country_iso3 = ?
              AND dp.value IS NOT NULL
            ORDER BY dp.date ASC
            """,
            (country,),
        )

        if not fdi_rows or len(fdi_rows) < 2:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient FDI data",
            }

        dates = [r["date"] for r in fdi_rows]
        values = np.array([float(r["value"]) for r in fdi_rows], dtype=float)

        latest_fdi = float(values[-1])
        t = np.arange(len(values), dtype=float)
        slope, _, r_value, p_value, _ = linregress(t, values)

        # Manufacturing trend for linkage quality
        manf_declining = False
        manf_pct = None
        manf_slope = None
        if manf_rows and len(manf_rows) >= 3:
            manf_vals = np.array([float(r["value"]) for r in manf_rows], dtype=float)
            manf_pct = float(manf_vals[-1])
            tm = np.arange(len(manf_vals), dtype=float)
            manf_slope_val, *_ = linregress(tm, manf_vals)
            manf_slope = float(manf_slope_val)
            manf_declining = manf_slope < 0

        level_score = max(0.0, 10.0 - latest_fdi) * 8.0
        trend_adjustment = -float(slope) * 200.0
        linkage_bonus = -20.0 if (float(slope) < 0 and manf_declining) else 0.0

        score = float(np.clip(level_score + trend_adjustment + linkage_bonus, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "country": country,
            "fdi_pct_gdp_latest": round(latest_fdi, 3),
            "latest_year": dates[-1],
            "fdi_slope_pp_per_year": round(float(slope), 4),
            "fdi_r_squared": round(float(r_value ** 2), 4),
            "fdi_p_value": round(float(p_value), 4),
            "n_obs": len(values),
            "manufacturing_pct_gdp": round(manf_pct, 2) if manf_pct is not None else None,
            "manufacturing_slope": round(manf_slope, 4) if manf_slope is not None else None,
            "linkage_stress": manf_declining and float(slope) < 0,
            "fdi_trend": "improving" if float(slope) > 0 else "declining",
            "fdi_tier": (
                "high inflows" if latest_fdi >= 5
                else "moderate inflows" if latest_fdi >= 2
                else "low inflows" if latest_fdi >= 0
                else "net outflows"
            ),
        }
