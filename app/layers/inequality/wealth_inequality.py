"""Wealth Inequality module.

Proxies wealth concentration using financial inclusion indicators.
Where credit and banking access are low, wealth tends to be highly concentrated
in a small segment able to access formal financial markets.

Indicators:
- FS.AST.PRVT.GD.ZS: Domestic credit to private sector (% of GDP).
  High credit relative to GDP may indicate credit is accessible broadly, or
  conversely, concentrated in the wealthy. Used here as a concentration signal
  when paired with low account ownership.
- FX.OWN.TOTL.ZS: Account ownership at a financial institution (% of adults 15+).
  Low account ownership = wealth exclusion of the poor.

Score = (1 - account_ownership/100) * 60 + credit_concentration_penalty.
Credit concentration penalty: clip((credit_gdp - 50) / 100 * 20, 0, 20) when
account_ownership < 50 (credit concentrated but access low).

Sources: World Bank WDI (FS.AST.PRVT.GD.ZS, FX.OWN.TOTL.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class WealthInequality(LayerBase):
    layer_id = "lIQ"
    name = "Wealth Inequality"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        credit_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FS.AST.PRVT.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        account_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FX.OWN.TOTL.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not account_rows and not credit_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        account_ownership = float(account_rows[0]["value"]) if account_rows else 50.0
        credit_gdp = float(credit_rows[0]["value"]) if credit_rows else 50.0
        has_account = bool(account_rows)
        has_credit = bool(credit_rows)

        # Financial exclusion component: low account ownership = high wealth inequality
        exclusion_score = float(np.clip((1.0 - account_ownership / 100.0) * 60.0, 0, 60))

        # Credit concentration: high credit_gdp + low account ownership = concentrated wealth
        if account_ownership < 50.0:
            credit_penalty = float(np.clip((credit_gdp - 50.0) / 100.0 * 20.0, 0, 20))
        else:
            credit_penalty = 0.0

        score = float(np.clip(exclusion_score + credit_penalty, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "account_ownership_pct": round(account_ownership, 2),
            "private_credit_pct_gdp": round(credit_gdp, 2),
            "account_source": "observed" if has_account else "imputed_default",
            "credit_source": "observed" if has_credit else "imputed_default",
            "exclusion_score": round(exclusion_score, 2),
            "credit_concentration_penalty": round(credit_penalty, 2),
            "interpretation": {
                "low_financial_inclusion": account_ownership < 50,
                "high_credit_relative_to_access": credit_gdp > 50 and account_ownership < 50,
            },
        }
