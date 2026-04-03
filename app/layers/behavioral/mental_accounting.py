"""Mental Accounting module.

Fiscal mental accounting: treating windfall resource revenues separately from tax revenue.
High natural resource rents combined with low tax effort indicates governments
mentally account for resource windfalls as separate from regular fiscal effort.

Score = clip((resource_rents / tax_revenue) * 20, 0, 100) when resource_rents > 5%

Sources: WDI NY.GDP.TOTL.RT.ZS (Total natural resources rents, % GDP),
         WDI GC.TAX.TOTL.GD.ZS (Tax revenue, % GDP)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

RESOURCE_THRESHOLD = 5.0  # percent of GDP


class MentalAccounting(LayerBase):
    layer_id = "l13"
    name = "Mental Accounting"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        resource_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.TOTL.RT.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        tax_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.TAX.TOTL.GD.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not resource_rows or len(resource_rows) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient resource rents data"}
        if not tax_rows or len(tax_rows) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient tax revenue data"}

        resource_map = {r["date"]: float(r["value"]) for r in resource_rows}
        tax_map = {r["date"]: float(r["value"]) for r in tax_rows}
        common_dates = sorted(set(resource_map) & set(tax_map))

        if len(common_dates) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient overlapping dates"}

        resource_vals = np.array([resource_map[d] for d in common_dates])
        tax_vals = np.array([tax_map[d] for d in common_dates])

        mean_resource = float(np.mean(resource_vals))
        mean_tax = float(np.mean(tax_vals))

        if mean_tax < 1e-10:
            return {"score": None, "signal": "UNAVAILABLE", "error": "zero tax revenue"}

        ratio = mean_resource / mean_tax

        # High resource rents relative to tax effort = mental accounting problem
        if mean_resource < RESOURCE_THRESHOLD:
            score = float(np.clip(ratio * 10, 0, 30))
        else:
            score = float(np.clip(ratio * 20, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(common_dates),
            "period": f"{common_dates[0]} to {common_dates[-1]}",
            "mean_resource_rents_pct_gdp": round(mean_resource, 2),
            "mean_tax_revenue_pct_gdp": round(mean_tax, 2),
            "resource_to_tax_ratio": round(ratio, 4),
            "resource_threshold_pct": RESOURCE_THRESHOLD,
            "interpretation": "High resource rents relative to tax effort signals fiscal mental accounting",
        }
