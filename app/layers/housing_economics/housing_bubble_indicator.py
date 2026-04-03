"""Housing Bubble Indicator module.

Price-to-rent overvaluation signal. Uses domestic credit to private sector
(FS.AST.PRVT.GD.ZS) as price proxy and Gini index (SI.POV.GINI) as a
demand-concentration proxy. Rapid credit expansion against concentrated
demand (high Gini) amplifies bubble risk. A z-score of recent credit growth
drives the primary signal; deviation of credit/GDP above 100 adds tail risk.

Score = clip((credit_growth_zscore * 25) + max(0, credit_gdp - 100) * 0.35 + 40, 0, 100)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class HousingBubbleIndicator(LayerBase):
    layer_id = "lHO"
    name = "Housing Bubble Indicator"

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

        if not credit_rows or len(credit_rows) < 5:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient credit data for housing bubble indicator",
            }

        credit_vals = np.array([float(r["value"]) for r in credit_rows])
        credit_changes = np.diff(credit_vals)

        if len(credit_changes) < 3:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient credit change observations for bubble detection",
            }

        mean_chg = float(np.mean(credit_changes))
        std_chg = float(np.std(credit_changes))
        latest_chg = float(credit_changes[-1])
        credit_growth_zscore = (latest_chg - mean_chg) / (std_chg + 1e-10)

        latest_credit_gdp = float(credit_vals[-1])

        # Gini amplifier: concentrated demand raises bubble risk
        gini_amplifier = 0.0
        gini_latest = None
        if gini_rows and len(gini_rows) >= 1:
            gini_vals = np.array([float(r["value"]) for r in gini_rows])
            gini_latest = float(gini_vals[-1])
            if gini_latest > 40:
                gini_amplifier = (gini_latest - 40) * 0.2

        raw_score = (credit_growth_zscore * 25) + max(0, latest_credit_gdp - 100) * 0.35 + 40 + gini_amplifier
        score = float(np.clip(raw_score, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "private_credit_gdp_pct": round(latest_credit_gdp, 2),
            "credit_growth_zscore": round(credit_growth_zscore, 3),
            "gini_index": round(gini_latest, 2) if gini_latest is not None else None,
            "gini_amplifier": round(gini_amplifier, 3),
            "n_credit_obs": len(credit_rows),
            "n_gini_obs": len(gini_rows) if gini_rows else 0,
            "methodology": "score = clip((credit_z * 25) + max(0, credit_gdp - 100) * 0.35 + gini_amp + 40, 0, 100)",
        }
