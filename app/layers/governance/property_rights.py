"""Property Rights module.

Measures property rights protection using Rule of Law (RL.EST) and the
Legal Rights Index (IC.LGL.CRED.XQ, strength of legal rights 0-12).

Weak rule of law + weak legal rights = high property rights risk.

Composite score construction:
  1. RL component: score_rl = clip(50 - rl * 20, 0, 100)
  2. Legal rights component: score_lr = clip((12 - lr) / 12 * 100, 0, 100)
     (lower legal rights index = higher stress)
  3. If both available: score = 0.6 * score_rl + 0.4 * score_lr
     If only RL available: score = score_rl
     If only LR available: score = score_lr

Sources: World Bank WDI (RL.EST, IC.LGL.CRED.XQ)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PropertyRights(LayerBase):
    layer_id = "lGV"
    name = "Property Rights"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT ds.series_id, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('RL.EST', 'IC.LGL.CRED.XQ')
            ORDER BY ds.series_id, dp.date
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        series: dict[str, list[float]] = {}
        series_dates: dict[str, list[str]] = {}
        for r in rows:
            sid = r["series_id"]
            series.setdefault(sid, []).append(float(r["value"]))
            series_dates.setdefault(sid, []).append(r["date"])

        latest: dict[str, float] = {k: v[-1] for k, v in series.items()}

        if not latest:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        score_rl = None
        score_lr = None

        if "RL.EST" in latest:
            score_rl = float(np.clip(50.0 - latest["RL.EST"] * 20.0, 0.0, 100.0))

        if "IC.LGL.CRED.XQ" in latest:
            lr = latest["IC.LGL.CRED.XQ"]
            score_lr = float(np.clip((12.0 - lr) / 12.0 * 100.0, 0.0, 100.0))

        if score_rl is not None and score_lr is not None:
            score = 0.6 * score_rl + 0.4 * score_lr
        elif score_rl is not None:
            score = score_rl
        else:
            score = score_lr

        all_dates = [d for dates in series_dates.values() for d in dates]

        return {
            "score": round(score, 1),
            "country": country,
            "rl_latest": round(latest["RL.EST"], 4) if "RL.EST" in latest else None,
            "legal_rights_index": round(latest["IC.LGL.CRED.XQ"], 2)
            if "IC.LGL.CRED.XQ" in latest
            else None,
            "score_rl_component": round(score_rl, 2) if score_rl is not None else None,
            "score_lr_component": round(score_lr, 2) if score_lr is not None else None,
            "indicators_used": list(latest.keys()),
            "period": f"{min(all_dates)} to {max(all_dates)}",
            "note": "RL.EST: -2.5 to +2.5. IC.LGL.CRED.XQ: 0 (weak) to 12 (strong rights)",
        }
