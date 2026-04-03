"""Top Income Share module.

Proxies top income concentration using Gini coefficient combined with fiscal
progressivity (income tax revenue share).

Logic:
- High Gini signals broad inequality.
- Low income tax share of total revenue signals weak redistribution, which
  correlates with top-end concentration.
- Combined: high Gini + low income tax ratio = strong top concentration signal.

Score = gini_component + (1 - progressivity_ratio) * 40.

Sources:
- SI.POV.GINI: Gini index (World Bank WDI)
- GC.TAX.YPKG.ZS: Taxes on income, profits and capital gains (% of revenue)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class TopIncomeShare(LayerBase):
    layer_id = "lIQ"
    name = "Top Income Share"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        gini_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SI.POV.GINI'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        tax_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.TAX.YPKG.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not gini_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        gini = float(gini_rows[0]["value"])

        if tax_rows:
            income_tax_share = float(tax_rows[0]["value"])
            has_tax = True
        else:
            income_tax_share = 20.0  # assume moderate progressivity when missing
            has_tax = False

        # Gini component: score rises with Gini above 30
        gini_score = float(np.clip((gini - 25.0) / 35.0 * 60.0, 0, 60))

        # Progressivity component: income tax share 0-100% of revenue
        # Low share (< 20%) = high concentration signal
        prog_score = float(np.clip((30.0 - income_tax_share) / 30.0 * 40.0, 0, 40))

        score = float(np.clip(gini_score + prog_score, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "gini": round(gini, 2),
            "income_tax_share_pct_revenue": round(income_tax_share, 2),
            "income_tax_source": "observed" if has_tax else "imputed_default",
            "gini_component": round(gini_score, 2),
            "progressivity_component": round(prog_score, 2),
            "interpretation": {
                "high_gini": gini > 40,
                "low_progressivity": income_tax_share < 20,
            },
        }
