"""Intergovernmental Transfer Dependency module.

Measures subnational dependency on central transfers by examining the ratio
of central government transfers (GC.XPN.TRFT.ZS, transfers as % of total
expense) to total government expenditure (GC.XPN.TOTL.GD.ZS, expense % GDP).
High transfer dependency signals weak own-source revenue and vulnerability
to central government fiscal decisions.

Score reflects dependency: high score = high transfer dependency.
Uses transfers-to-GDP proxy = (GC.XPN.TRFT.ZS / 100) * GC.XPN.TOTL.GD.ZS.
Score = clip(transfers_gdp / 15 * 100, 0, 100), anchoring 15% GDP at full stress.

Sources: WDI GC.XPN.TRFT.ZS, GC.XPN.TOTL.GD.ZS.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_HIGH_DEPENDENCY_THRESHOLD = 15.0  # transfers > 15% GDP = full dependency stress


class IntergovernmentalTransferDependency(LayerBase):
    layer_id = "lLG"
    name = "Intergovernmental Transfer Dependency"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        trft_code = "GC.XPN.TRFT.ZS"
        trft_name = "expense transfers"
        trft_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (trft_code, f"%{trft_name}%"),
        )

        exp_code = "GC.XPN.TOTL.GD.ZS"
        exp_name = "general government expense"
        exp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (exp_code, f"%{exp_name}%"),
        )

        if not trft_rows and not exp_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no transfer dependency data"}

        trft_pct_exp = float(trft_rows[0]["value"]) if trft_rows else None
        exp_pct_gdp = float(exp_rows[0]["value"]) if exp_rows else None

        if trft_pct_exp is not None and exp_pct_gdp is not None:
            transfers_gdp = (trft_pct_exp / 100.0) * exp_pct_gdp
        elif trft_pct_exp is not None:
            # Assume ~25% GDP average expenditure
            transfers_gdp = (trft_pct_exp / 100.0) * 25.0
        else:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient transfer data"}

        score = float(np.clip(transfers_gdp / _HIGH_DEPENDENCY_THRESHOLD * 100.0, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "transfers_pct_expense": round(trft_pct_exp, 2) if trft_pct_exp is not None else None,
            "expense_pct_gdp": round(exp_pct_gdp, 2) if exp_pct_gdp is not None else None,
            "transfers_pct_gdp": round(transfers_gdp, 2),
            "dependency_threshold_pct_gdp": _HIGH_DEPENDENCY_THRESHOLD,
            "interpretation": (
                "Extreme transfer dependency: subnational units almost fully reliant on center"
                if score > 75
                else "High dependency: limited own-source revenue autonomy" if score > 50
                else "Moderate transfer dependency" if score > 25
                else "Low transfer dependency: reasonable own-source revenue base"
            ),
            "_sources": ["WDI:GC.XPN.TRFT.ZS", "WDI:GC.XPN.TOTL.GD.ZS"],
        }
