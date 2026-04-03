"""Funded vs Unfunded Risk module.

Measures PAYGO (pay-as-you-go) insolvency risk by combining old-age
dependency with fiscal deficit. High dependency means the contributor base
is shrinking relative to beneficiaries. A fiscal deficit simultaneously
limits government's ability to fund shortfalls. Together they indicate
PAYGO system collapse risk.

Score = clip(dependency * max(0, -deficit) / 5, 0, 100)

Sources: WDI SP.POP.DPND.OL (old-age dependency ratio, per 100 working-age),
         WDI GC.BAL.CASH.GD.ZS (cash surplus/deficit % of GDP, negative = deficit)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class FundedVsUnfundedRisk(LayerBase):
    layer_id = "lPS"
    name = "Funded vs Unfunded Risk"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        dependency_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SP.POP.DPND.OL'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        deficit_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.BAL.CASH.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not dependency_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no dependency ratio data"}
        if not deficit_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no fiscal balance data"}

        dep_vals = [float(r["value"]) for r in dependency_rows if r["value"] is not None]
        def_vals = [float(r["value"]) for r in deficit_rows if r["value"] is not None]

        if not dep_vals or not def_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient valid data"}

        dependency = float(np.mean(dep_vals))
        fiscal_balance = float(np.mean(def_vals))  # negative = deficit

        # Deficit magnitude (positive = deficit)
        deficit_magnitude = max(0.0, -fiscal_balance)

        score = float(np.clip(dependency * deficit_magnitude / 5.0, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "old_age_dependency_ratio": round(dependency, 2),
            "fiscal_balance_pct_gdp": round(fiscal_balance, 2),
            "deficit_magnitude": round(deficit_magnitude, 2),
            "paygo_insolvency_risk": score > 50,
            "interpretation": (
                "high PAYGO insolvency risk" if score > 75
                else "elevated PAYGO risk" if score > 50
                else "moderate PAYGO risk" if score > 25
                else "PAYGO system sustainable"
            ),
            "sources": ["WDI SP.POP.DPND.OL", "WDI GC.BAL.CASH.GD.ZS"],
        }
