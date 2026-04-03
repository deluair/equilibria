"""Countercyclical Fiscal Policy module.

Measures whether fiscal policy amplifies or dampens the business cycle.
A procyclical fiscal stance (expanding spending during booms, cutting during
recessions) is destabilising and raises macroeconomic stress.

Methodology:
- Query GC.XPN.TOTL.GD.ZS (government expenditure, % GDP) changes.
- Query NY.GDP.MKTP.KD.ZG (GDP growth, %).
- Compute Pearson correlation between expenditure changes and GDP growth.
- Positive correlation = procyclical (amplifies cycles) -> higher stress.
- Score = clip(max(0, corr) * 80, 0, 100).

Sources: World Bank WDI (GC.XPN.TOTL.GD.ZS, NY.GDP.MKTP.KD.ZG)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import pearsonr

from app.layers.base import LayerBase


class CountercyclicalFiscal(LayerBase):
    layer_id = "lFP"
    name = "Countercyclical Fiscal Policy"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        exp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.XPN.TOTL.GD.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        gdp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.MKTP.KD.ZG'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not exp_rows or not gdp_rows or len(exp_rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        exp_map = {r["date"]: float(r["value"]) for r in exp_rows}
        gdp_map = {r["date"]: float(r["value"]) for r in gdp_rows}
        common_dates = sorted(set(exp_map) & set(gdp_map))

        if len(common_dates) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient overlapping data"}

        exp_vals = np.array([exp_map[d] for d in common_dates])
        gdp_vals = np.array([gdp_map[d] for d in common_dates])

        exp_changes = np.diff(exp_vals)
        gdp_aligned = gdp_vals[1:]

        if len(exp_changes) < 4:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient variation"}

        corr, p_value = pearsonr(exp_changes, gdp_aligned)

        score = float(np.clip(max(0.0, float(corr)) * 80, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "expenditure_gdp_corr": round(float(corr), 4),
            "corr_p_value": round(float(p_value), 4),
            "procyclical": corr > 0,
            "n_obs": len(common_dates),
            "period": f"{common_dates[0]} to {common_dates[-1]}",
            "indicators": ["GC.XPN.TOTL.GD.ZS", "NY.GDP.MKTP.KD.ZG"],
        }
