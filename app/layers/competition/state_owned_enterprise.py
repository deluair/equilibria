"""State-Owned Enterprise Dominance module.

Proxies SOE dominance via the gap between government expenditure
and private investment as shares of GDP.

Rationale:
- Very high government spending (GC.XPN.TOTL.GD.ZS) combined with
  low private fixed investment (NE.GDI.PRVT.ZS) indicates that state
  entities crowd out private sector activity (crowding-out effect).
- The SOE dominance index = govt_exp_share - priv_inv_share.
  Large positive values -> state dominates productive activity.

Score = clip((govt_exp - priv_inv + 10) * 2.5, 0, 100).
Calibrated so that equal shares (difference = 0) -> score ~25;
govt 40%/priv 5% (difference 35%) -> score ~100.

Sources: WDI (GC.XPN.TOTL.GD.ZS, NE.GDI.PRVT.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class StateOwnedEnterprise(LayerBase):
    layer_id = "lCO"
    name = "State-Owned Enterprise Dominance"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        govt_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.XPN.TOTL.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        priv_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.GDI.PRVT.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not govt_rows and not priv_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no SOE proxy data"}

        def latest_value(rows) -> float | None:
            for r in rows:
                if r["value"] is not None:
                    try:
                        return float(r["value"])
                    except (TypeError, ValueError):
                        pass
            return None

        govt_exp = latest_value(govt_rows)
        priv_inv = latest_value(priv_rows)

        if govt_exp is None and priv_inv is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "missing SOE inputs"}

        g = govt_exp if govt_exp is not None else 25.0  # neutral fallback
        p = priv_inv if priv_inv is not None else 15.0

        dominance_gap = g - p
        score = float(np.clip((dominance_gap + 10) * 2.5, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "govt_expenditure_pct_gdp": round(g, 2),
            "private_investment_pct_gdp": round(p, 2),
            "dominance_gap": round(dominance_gap, 2),
            "interpretation": (
                "private-sector-led economy" if score < 33
                else "mixed economy" if score < 66
                else "state-dominated economy"
            ),
            "reference": "Crowding-out literature: Bernheim (1989), Friedman (1978)",
        }
