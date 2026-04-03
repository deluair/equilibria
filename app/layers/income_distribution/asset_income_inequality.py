"""Asset Income Inequality module.

Measures whether financial deepening concentrates asset income at the top.
Deep private credit markets combined with high Gini suggests financial
returns are captured by asset-owning households, not wage earners.

Score = clip(private_credit * gini / 3000, 0, 100).

Sources: WDI (FS.AST.PRVT.GD.ZS, SI.POV.GINI)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class AssetIncomeInequality(LayerBase):
    layer_id = "lID"
    name = "Asset Income Inequality"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        credit_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FS.AST.PRVT.GD.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        gini_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SI.POV.GINI'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not credit_rows and not gini_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        if credit_rows:
            credit_vals = np.array([float(r["value"]) for r in credit_rows])
            private_credit = float(np.mean(credit_vals[-3:]))
            credit_trend = float(np.polyfit(np.arange(len(credit_vals)), credit_vals, 1)[0]) \
                if len(credit_vals) >= 3 else 0.0
            credit_period = f"{credit_rows[0]['date']} to {credit_rows[-1]['date']}"
        else:
            private_credit = 50.0
            credit_trend = 0.0
            credit_period = None

        if gini_rows:
            gini_vals = np.array([float(r["value"]) for r in gini_rows])
            gini = float(np.mean(gini_vals[-3:]))
            gini_trend = float(np.polyfit(np.arange(len(gini_vals)), gini_vals, 1)[0]) \
                if len(gini_vals) >= 3 else 0.0
            gini_period = f"{gini_rows[0]['date']} to {gini_rows[-1]['date']}"
        else:
            gini = 40.0
            gini_trend = 0.0
            gini_period = None

        score = float(np.clip(private_credit * gini / 3000.0, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "private_credit_pct_gdp": round(private_credit, 2),
            "credit_period": credit_period,
            "credit_trend_per_year": round(credit_trend, 4),
            "gini": round(gini, 2),
            "gini_period": gini_period,
            "gini_trend_per_year": round(gini_trend, 4),
            "asset_concentration_index": round(private_credit * gini / 3000.0, 4),
            "interpretation": (
                "deep credit + high Gini = financial returns concentrated among asset owners; "
                "score rises with both financial depth and inequality"
            ),
        }
