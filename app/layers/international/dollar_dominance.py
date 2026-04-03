"""Dollar Dominance module.

Measures USD dependency via external debt burden relative to import reserve coverage.
A country with high external debt (mostly denominated in foreign currency) and thin
reserves faces acute dollar dominance vulnerability: rollover risk, balance-sheet
mismatches, and susceptibility to Fed policy spillovers (Eichengreen et al. 2002;
Obstfeld 2015).

Score = weighted combination of external debt stress + reserve inadequacy.

Sources: WDI (DT.DOD.DECT.GD.ZS, FI.RES.TOTL.MO)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

# External debt/GDP above this is high vulnerability
DEBT_HIGH_THRESHOLD = 60.0  # percent of GDP
# Reserves below this (months of imports) is inadequate
RESERVES_LOW_THRESHOLD = 3.0  # months


class DollarDominance(LayerBase):
    layer_id = "lIN"
    name = "Dollar Dominance"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        debt_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'DT.DOD.DECT.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        reserves_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FI.RES.TOTL.MO'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not debt_rows or not reserves_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data for external debt or reserves",
            }

        debt_values = [float(r["value"]) for r in debt_rows if r["value"] is not None]
        res_values = [float(r["value"]) for r in reserves_rows if r["value"] is not None]

        if not debt_values or not res_values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no valid numeric data",
            }

        latest_debt = debt_values[0]
        latest_reserves = res_values[0]

        # Debt penalty: excess above threshold, scaled 0-60
        debt_penalty = float(np.clip((latest_debt - DEBT_HIGH_THRESHOLD) * 0.8, 0, 60))

        # Reserve inadequacy penalty: shortfall below threshold, scaled 0-40
        reserve_penalty = float(
            np.clip((RESERVES_LOW_THRESHOLD - latest_reserves) * 10, 0, 40)
        )

        score = float(np.clip(debt_penalty + reserve_penalty, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "external_debt_pct_gdp": round(latest_debt, 3),
            "reserves_months_imports": round(latest_reserves, 3),
            "debt_threshold": DEBT_HIGH_THRESHOLD,
            "reserves_threshold": RESERVES_LOW_THRESHOLD,
        }
