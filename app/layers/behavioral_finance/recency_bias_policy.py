"""Recency Bias Policy module.

Fiscal policy procyclicality as evidence of recency bias. Policymakers who
over-weight recent GDP performance increase spending during booms and cut
during recessions, amplifying cycles rather than stabilizing them.

Measured as the correlation between government expenditure changes and GDP growth.
Positive (procyclical) correlation indicates recency-biased fiscal policy.

Sources: WDI GC.XPN.TOTL.GD.ZS (general government expenditure % of GDP),
         WDI NY.GDP.MKTP.KD.ZG (GDP growth annual %)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class RecencyBiasPolicy(LayerBase):
    layer_id = "lBF"
    name = "Recency Bias Policy"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        exp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            ("GC.XPN.TOTL.GD.ZS", "%government expenditure%"),
        )
        gdp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            ("NY.GDP.MKTP.KD.ZG", "%GDP growth%"),
        )

        if not exp_rows or len(exp_rows) < 5 or not gdp_rows or len(gdp_rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        n = min(len(exp_rows), len(gdp_rows))
        exp_vals = np.array([float(r["value"]) for r in exp_rows[:n]])[::-1]
        gdp_vals = np.array([float(r["value"]) for r in gdp_rows[:n]])[::-1]

        exp_changes = np.diff(exp_vals)
        gdp_growth = gdp_vals[1:]

        if len(exp_changes) < 4:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient overlapping obs"}

        corr_matrix = np.corrcoef(exp_changes, gdp_growth)
        procyclicality = float(corr_matrix[0, 1])

        # Positive procyclicality = recency bias (amplifying policy)
        score = float(np.clip(max(0.0, procyclicality) * 100, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": n,
            "expenditure_gdp_growth_corr": round(procyclicality, 3),
            "interpretation": "Positive correlation between expenditure changes and GDP growth indicates procyclical, recency-biased fiscal policy",
        }
