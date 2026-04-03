"""Inflation Target Credibility: deviation from target and expectations anchoring.

Methodology
-----------
Credibility has two components (Demertzis, Marcellino, Viegi 2012):

1. Level credibility: how close is actual/expected inflation to target?
   cred_level = 1 - |pi - pi*| / max(pi*, 0.5)  (bounded 0-1)

2. Anchoring: do long-run expectations respond to short-run shocks?
   Regress delta_E[pi_long] on delta_pi_actual
   anchoring_coef near 0 -> well anchored; near 1 -> poorly anchored

Score = clip((1 - cred_level) * 50 + anchoring_coef * 50, 0, 100)
  Both perfectly credible -> score 0 (STABLE)
  Large deviations + unanchored -> score 100 (CRISIS)

Sources: WDI FP.CPI.TOTL.ZG, IMF WEO PCPIEPCH (inflation expectations)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class InflationTargetCredibility(LayerBase):
    layer_id = "lMY"
    name = "Inflation Target Credibility"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")
        pi_target = kwargs.get("pi_target", 2.0)
        lookback = kwargs.get("lookback_years", 10)

        inflation_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FP.CPI.TOTL.ZG'
              AND dp.date >= date('now', ?)
            ORDER BY dp.date
            """,
            (country, f"-{lookback} years"),
        )

        exp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'PCPIEPCH'
              AND dp.date >= date('now', ?)
            ORDER BY dp.date
            """,
            (country, f"-{lookback} years"),
        )

        if not inflation_rows or len(inflation_rows) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient inflation data"}

        pi_vals = np.array([float(r["value"]) for r in inflation_rows])
        pi_latest = float(pi_vals[-1])

        cred_level = float(np.clip(1.0 - abs(pi_latest - pi_target) / max(pi_target, 0.5), 0, 1))
        mean_deviation = float(np.mean(np.abs(pi_vals - pi_target)))

        anchoring_coef: float | None = None
        if exp_rows and len(exp_rows) >= 4:
            inf_map = {r["date"]: float(r["value"]) for r in inflation_rows}
            exp_map = {r["date"]: float(r["value"]) for r in exp_rows}
            common = sorted(set(inf_map) & set(exp_map))
            if len(common) >= 4:
                pi_common = np.array([inf_map[d] for d in common])
                exp_common = np.array([exp_map[d] for d in common])
                d_exp = np.diff(exp_common)
                d_pi = np.diff(pi_common)
                if np.std(d_pi) > 1e-10:
                    anchoring_coef = float(np.polyfit(d_pi, d_exp, 1)[0])

        anch = float(np.clip(abs(anchoring_coef), 0, 1)) if anchoring_coef is not None else 0.5

        score = float(np.clip((1.0 - cred_level) * 50 + anch * 50, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "pi_target": pi_target,
            "inflation_latest": round(pi_latest, 2),
            "deviation_from_target_pp": round(pi_latest - pi_target, 2),
            "mean_abs_deviation_pp": round(mean_deviation, 2),
            "credibility_level_index": round(cred_level, 3),
            "anchoring_coefficient": round(anchoring_coef, 4) if anchoring_coef is not None else None,
            "well_anchored": abs(anchoring_coef) < 0.3 if anchoring_coef is not None else None,
            "n_obs": len(inflation_rows),
            "period": f"{inflation_rows[0]['date']} to {inflation_rows[-1]['date']}",
        }
