"""Financial Literacy Proxy module.

Account ownership combined with secondary education enrollment as a proxy
for financial literacy. Low account ownership despite adequate education
signals a literacy-access gap; low education constrains financial capability.

Sources: WDI FX.OWN.TOTL.ZS (account ownership % age 15+),
         WDI SE.SEC.ENRR (secondary school enrollment gross %)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class FinancialLiteracyProxy(LayerBase):
    layer_id = "lBF"
    name = "Financial Literacy Proxy"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        account_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            ("FX.OWN.TOTL.ZS", "%account ownership%"),
        )
        edu_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            ("SE.SEC.ENRR", "%secondary%enrollment%"),
        )

        if not account_rows or not edu_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        account_val = float(account_rows[0]["value"])
        edu_val = float(edu_rows[0]["value"])

        # Financial literacy = account ownership * normalized education
        edu_norm = np.clip(edu_val / 100.0, 0, 1)
        literacy_index = account_val * edu_norm  # 0-100

        # Low literacy = high behavioral risk score (invert)
        score = float(np.clip(100 - literacy_index, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "account_ownership_pct": round(account_val, 2),
            "secondary_enrollment_pct": round(edu_val, 2),
            "literacy_index": round(literacy_index, 2),
            "interpretation": "Lower literacy index (account ownership x education) implies higher behavioral finance risk",
        }
