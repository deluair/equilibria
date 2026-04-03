"""Policy Rate Gap: actual policy rate vs Taylor rule implied rate.

Methodology
-----------
Taylor (1993) rule: i* = r* + pi + 0.5*(pi - pi*) + 0.5*y_gap
  where r* = equilibrium real rate (assumed 2%)
        pi = current inflation
        pi* = inflation target (assumed 2%)
        y_gap = output gap (% of potential)

Gap = actual_rate - taylor_implied_rate
  Positive gap -> policy tighter than Taylor suggests (restrictive)
  Negative gap -> policy looser than Taylor suggests (accommodative)

Score = clip(abs(gap) * 10, 0, 100)
  gap = 0   -> score 0  (STABLE: on target)
  gap = 5pp -> score 50 (WATCH)
  gap = 10pp-> score 100 (CRISIS: severe misalignment)

Sources: World Bank WDI / FRED
  FP.CPI.TOTL.ZG - CPI inflation (annual %)
  NY.GDP.DEFL.KD.ZG - GDP deflator (alternative)
  Output gap: IMF WEO (NGAP_NPGDP) or estimated via HP filter
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PolicyRateGap(LayerBase):
    layer_id = "lMY"
    name = "Policy Rate Gap"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")
        pi_target = kwargs.get("pi_target", 2.0)
        r_star = kwargs.get("r_star", 2.0)

        inflation_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FP.CPI.TOTL.ZG'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        output_gap_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NGAP_NPGDP'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        policy_rate_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FIDR'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not inflation_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no inflation data"}
        if not policy_rate_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no policy rate data"}

        pi = float(inflation_rows[0]["value"])
        actual_rate = float(policy_rate_rows[0]["value"])
        y_gap = float(output_gap_rows[0]["value"]) if output_gap_rows else 0.0

        taylor_implied = r_star + pi + 0.5 * (pi - pi_target) + 0.5 * y_gap
        gap = actual_rate - taylor_implied

        score = float(np.clip(abs(gap) * 10, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "actual_policy_rate": round(actual_rate, 2),
            "taylor_implied_rate": round(taylor_implied, 2),
            "policy_rate_gap_pp": round(gap, 2),
            "gap_direction": "restrictive" if gap > 0 else "accommodative",
            "inflation_used": round(pi, 2),
            "output_gap_used": round(y_gap, 2),
            "pi_target": pi_target,
            "r_star": r_star,
            "date": inflation_rows[0]["date"],
        }
