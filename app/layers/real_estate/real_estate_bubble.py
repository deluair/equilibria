"""Real Estate Bubble Detection module.

Uses credit growth and construction sector proxies to detect bubble risk.
Queries domestic credit/GDP (FS.AST.DOMS.GD.ZS) and manufacturing value
added (NV.IND.MANF.ZS) as industry proxy. Rapid credit expansion signals
bubble risk.

Score = clip((credit_growth_zscore * 30) + (credit_gdp - 100) * 0.3 + 50, 0, 100)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class RealEstateBubble(LayerBase):
    layer_id = "lRE"
    name = "Real Estate Bubble"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        credit_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FS.AST.DOMS.GD.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        manuf_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NV.IND.MANF.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not credit_rows or len(credit_rows) < 5:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient credit data for bubble detection",
            }

        credit_vals = np.array([float(r["value"]) for r in credit_rows])

        # Credit growth z-score (rapid expansion = bubble risk)
        credit_changes = np.diff(credit_vals)
        if len(credit_changes) < 3:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient credit change observations",
            }

        mean_change = float(np.mean(credit_changes))
        std_change = float(np.std(credit_changes))
        latest_change = float(credit_changes[-1])
        credit_growth_zscore = (latest_change - mean_change) / (std_change + 1e-10)

        latest_credit_gdp = float(credit_vals[-1])

        # Manufacturing proxy: low manufacturing + high credit = bubble risk
        manuf_penalty = 0.0
        manuf_latest = None
        if manuf_rows and len(manuf_rows) >= 2:
            manuf_vals = np.array([float(r["value"]) for r in manuf_rows])
            manuf_latest = float(manuf_vals[-1])
            # Low manufacturing share amplifies credit-driven bubble risk
            if manuf_latest < 15:
                manuf_penalty = (15 - manuf_latest) * 0.5

        raw_score = (credit_growth_zscore * 30) + max(0, latest_credit_gdp - 100) * 0.3 + 50 + manuf_penalty
        score = float(np.clip(raw_score, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "credit_gdp_pct": round(latest_credit_gdp, 2),
            "credit_growth_zscore": round(credit_growth_zscore, 3),
            "manufacturing_value_added_pct": round(manuf_latest, 2) if manuf_latest is not None else None,
            "n_credit_obs": len(credit_rows),
            "n_manuf_obs": len(manuf_rows) if manuf_rows else 0,
            "methodology": "z-score of credit growth + excess credit/GDP deviation + manufacturing penalty",
        }
