"""Migration-Remittance Cycle module.

Examines the migration-development feedback loop: whether remittance
inflows translate into productive investment or remain trapped in
household consumption.

High remittances relative to overall investment (gross capital
formation as % GDP) suggests a consumption trap: the diaspora
provides income support but does not catalyze domestic investment,
limiting the development multiplier of migration.

Score reflects remittance-investment imbalance: high remittances
with low investment = high stress (consumption trap).

Sources: WDI (BX.TRF.PWKR.DT.GD.ZS, NE.GDI.TOTL.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MigrationRemittanceCycle(LayerBase):
    layer_id = "lME"
    name = "Migration Remittance Cycle"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rem_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'BX.TRF.PWKR.DT.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        inv_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.GDI.TOTL.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not rem_rows and not inv_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        rem_vals = [float(r["value"]) for r in rem_rows if r["value"] is not None]
        inv_vals = [float(r["value"]) for r in inv_rows if r["value"] is not None]

        rem = float(np.mean(rem_vals)) if rem_vals else 0.0
        inv = float(np.mean(inv_vals)) if inv_vals else 20.0

        # Consumption trap: high remittances but low investment
        # If remittances exceed investment, extreme trap
        rem_score = float(np.clip(rem * 3, 0, 60))

        # Investment adequacy: below 20% GDP is low; penalty for shortfall
        inv_shortfall = max(0.0, 20.0 - inv)
        inv_score = float(np.clip(inv_shortfall * 2, 0, 40))

        score = rem_score + inv_score

        return {
            "score": round(score, 1),
            "country": country,
            "remittance_pct_gdp": round(rem, 2),
            "gross_capital_formation_pct_gdp": round(inv, 2),
            "rem_to_investment_ratio": round(rem / inv, 3) if inv > 0 else None,
            "components": {
                "remittance_dependency": round(rem_score, 2),
                "investment_gap": round(inv_score, 2),
            },
            "interpretation": (
                "consumption trap" if rem > inv
                else "partial trap" if score > 40
                else "healthy investment cycle"
            ),
        }
