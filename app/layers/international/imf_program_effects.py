"""IMF Program Effects module.

Proxy for IMF program necessity via reserve adequacy and fiscal balance stress.
Countries with low import reserve cover combined with fiscal deficits are the
canonical candidates for IMF balance-of-payments support programs (Bird 2002;
Dreher 2006; IMF 2023 reserve adequacy framework).

Score = 0.5 * reserve_stress + 0.5 * fiscal_stress, clipped to [0, 100].

Sources: WDI (FI.RES.TOTL.MO reserves months of imports, GC.BAL.CASH.GD.ZS fiscal balance)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

# IMF guideline: >= 3 months reserves is adequate
RESERVES_ADEQUATE_THRESHOLD = 3.0  # months of imports
# Fiscal deficit beyond this % of GDP signals fiscal stress
FISCAL_DEFICIT_THRESHOLD = -3.0  # percent of GDP


class IMFProgramEffects(LayerBase):
    layer_id = "lIN"
    name = "IMF Program Effects"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        reserves_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FI.RES.TOTL.MO'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        fiscal_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.BAL.CASH.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not reserves_rows and not fiscal_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no reserves or fiscal balance data",
            }

        reserves_values = [
            float(r["value"]) for r in reserves_rows if r["value"] is not None
        ]
        fiscal_values = [
            float(r["value"]) for r in fiscal_rows if r["value"] is not None
        ]

        # Reserve stress: below IMF adequacy threshold
        if reserves_values:
            avg_reserves = float(np.mean(reserves_values))
            reserve_stress = float(
                np.clip(
                    (RESERVES_ADEQUATE_THRESHOLD - avg_reserves) / RESERVES_ADEQUATE_THRESHOLD * 100,
                    0,
                    100,
                )
            )
        else:
            avg_reserves = None
            reserve_stress = 50.0  # unknown, assume moderate stress

        # Fiscal stress: deficit beyond threshold
        if fiscal_values:
            avg_fiscal = float(np.mean(fiscal_values))
            if avg_fiscal < FISCAL_DEFICIT_THRESHOLD:
                # Excess deficit beyond threshold, scaled 0-100
                fiscal_stress = float(
                    np.clip((FISCAL_DEFICIT_THRESHOLD - avg_fiscal) * 10, 0, 100)
                )
            else:
                fiscal_stress = 0.0
        else:
            avg_fiscal = None
            fiscal_stress = 50.0  # unknown, assume moderate stress

        score = 0.5 * reserve_stress + 0.5 * fiscal_stress

        return {
            "score": round(score, 1),
            "country": country,
            "avg_reserves_months": round(avg_reserves, 3) if avg_reserves is not None else None,
            "reserve_stress": round(reserve_stress, 1),
            "avg_fiscal_balance_pct_gdp": round(avg_fiscal, 3) if avg_fiscal is not None else None,
            "fiscal_stress": round(fiscal_stress, 1),
            "reserves_adequate": avg_reserves >= RESERVES_ADEQUATE_THRESHOLD
            if avg_reserves is not None
            else None,
            "fiscal_deficit": avg_fiscal < 0 if avg_fiscal is not None else None,
        }
