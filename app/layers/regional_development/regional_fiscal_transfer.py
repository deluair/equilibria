"""Regional Fiscal Transfer module.

Measures fiscal decentralization adequacy by comparing tax revenue capacity
(central government tax revenue % of GDP) with government expenditure burden
(% of GDP). When spending greatly exceeds own revenue, the system depends on
fiscal transfers -- often flowing to lagging regions -- indicating imbalanced
regional fiscal capacity.

Score = clip((expenditure - tax_revenue) / expenditure * 100, 0, 100)
If expenditure <= tax_revenue, score = 0 (fiscally self-sufficient).

Sources: WDI GC.TAX.TOTL.GD.ZS (tax revenue % of GDP),
         WDI GC.XPN.TOTL.GD.ZS (general government expenditure % of GDP)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class RegionalFiscalTransfer(LayerBase):
    layer_id = "lRD"
    name = "Regional Fiscal Transfer"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows_tax = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.TAX.TOTL.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        rows_exp = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.XPN.TOTL.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not rows_tax or not rows_exp:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        tax_map = {r["date"]: float(r["value"]) for r in rows_tax if r["value"] is not None}
        exp_map = {r["date"]: float(r["value"]) for r in rows_exp if r["value"] is not None}

        common_dates = sorted(set(tax_map) & set(exp_map), reverse=True)
        if not common_dates:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no overlapping dates"}

        gaps = []
        for d in common_dates:
            tax = tax_map[d]
            exp = exp_map[d]
            if exp > 0:
                gap_ratio = max(0.0, (exp - tax) / exp * 100)
                gaps.append(gap_ratio)

        if not gaps:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid gap observations"}

        mean_gap = float(np.mean(gaps))
        score = float(np.clip(mean_gap, 0, 100))

        latest = common_dates[0]
        return {
            "score": round(score, 1),
            "country": country,
            "latest_date": latest,
            "latest_tax_pct": round(tax_map[latest], 2),
            "latest_expenditure_pct": round(exp_map[latest], 2),
            "latest_transfer_dependency_pct": round(gaps[0], 2),
            "mean_transfer_dependency_pct": round(mean_gap, 2),
            "n_obs": len(gaps),
            "series": {
                "tax": "GC.TAX.TOTL.GD.ZS",
                "expenditure": "GC.XPN.TOTL.GD.ZS",
            },
            "interpretation": "high gap = fiscal transfer dependency across regions",
        }
