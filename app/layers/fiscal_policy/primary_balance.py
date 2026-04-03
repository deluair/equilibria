"""Primary Balance module.

Estimates the primary fiscal balance (fiscal balance net of interest payments).
A negative primary balance signals a structural fiscal weakness: the government
cannot cover non-interest spending from current revenues.

Methodology:
- Query GC.BAL.CASH.GD.ZS (fiscal balance, % GDP).
- Query GC.XPN.INTP.CN (interest payments, LCU) and GC.XPN.TOTL.CN (total
  expenditure, LCU) to estimate interest_share of expenditure.
- Primary balance proxy = fiscal_balance + interest_share * |fiscal_balance|
  (adds back interest cost relative to fiscal position).
- Negative primary balance -> structural weakness; score penalises accordingly.
- Score = clip(-primary_balance * 8, 0, 100).

Sources: World Bank WDI (GC.BAL.CASH.GD.ZS, GC.XPN.INTP.CN, GC.XPN.TOTL.CN)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PrimaryBalance(LayerBase):
    layer_id = "lFP"
    name = "Primary Balance"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        # Fiscal balance
        bal_rows = await db.fetch_all(
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

        # Interest payments
        intp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.XPN.INTP.CN'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        # Total expenditure
        totl_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.XPN.TOTL.CN'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not bal_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        fiscal_balance = float(bal_rows[0]["value"])

        # Estimate interest share of expenditure
        interest_share = None
        if intp_rows and totl_rows:
            intp_val = float(intp_rows[0]["value"])
            totl_val = float(totl_rows[0]["value"])
            if totl_val and abs(totl_val) > 1e-10:
                interest_share = intp_val / totl_val

        # Primary balance estimate: fiscal balance + interest cost added back
        if interest_share is not None:
            primary_balance = fiscal_balance + interest_share * 100
        else:
            primary_balance = fiscal_balance  # fallback: use raw balance

        score = float(np.clip(-primary_balance * 8, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "fiscal_balance_pct_gdp": round(fiscal_balance, 3),
            "interest_share_of_expenditure": round(interest_share, 4)
            if interest_share is not None
            else None,
            "primary_balance_estimate": round(primary_balance, 3),
            "structural_weakness": primary_balance < 0,
            "indicators": ["GC.BAL.CASH.GD.ZS", "GC.XPN.INTP.CN", "GC.XPN.TOTL.CN"],
        }
