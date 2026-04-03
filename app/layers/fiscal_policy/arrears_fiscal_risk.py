"""Arrears Fiscal Risk module.

Uses the trend in interest payments as a share of total government expenditure
as a proxy for fiscal arrears risk. A rising interest burden signals that
debt service obligations are consuming an increasing share of the budget,
crowding out productive spending and raising the probability of payment arrears.

Methodology:
- Query GC.XPN.INTP.CN (interest payments, LCU).
- Query GC.XPN.TOTL.CN (total expenditure, LCU).
- Compute interest_pct = (interest / total) * 100 for each year.
- Score = clip(interest_pct * 5, 0, 100).

Sources: World Bank WDI (GC.XPN.INTP.CN, GC.XPN.TOTL.CN)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class ArrearsFiscalRisk(LayerBase):
    layer_id = "lFP"
    name = "Arrears Fiscal Risk"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        intp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.XPN.INTP.CN'
            ORDER BY dp.date
            """,
            (country,),
        )

        totl_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.XPN.TOTL.CN'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not intp_rows or not totl_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        intp_map = {r["date"]: float(r["value"]) for r in intp_rows}
        totl_map = {r["date"]: float(r["value"]) for r in totl_rows}
        common_dates = sorted(set(intp_map) & set(totl_map))

        if not common_dates:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no overlapping dates"}

        interest_pcts = []
        for d in common_dates:
            totl = totl_map[d]
            if abs(totl) > 1e-10:
                interest_pcts.append((intp_map[d] / totl) * 100)

        if not interest_pcts:
            return {"score": None, "signal": "UNAVAILABLE", "error": "zero expenditure"}

        interest_pct = float(interest_pcts[-1])

        # Trend in interest share
        slope = None
        if len(interest_pcts) >= 3:
            x = np.arange(len(interest_pcts), dtype=float)
            y = np.array(interest_pcts)
            slope_val, _, _, _, _ = linregress(x, y)
            slope = float(slope_val)

        score = float(np.clip(interest_pct * 5, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "interest_pct_expenditure": round(interest_pct, 3),
            "interest_share_trend_slope": round(slope, 4) if slope is not None else None,
            "rising_burden": (slope or 0.0) > 0,
            "n_obs": len(interest_pcts),
            "period": f"{common_dates[0]} to {common_dates[-1]}",
            "indicators": ["GC.XPN.INTP.CN", "GC.XPN.TOTL.CN"],
        }
