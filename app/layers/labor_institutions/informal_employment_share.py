"""Informal employment share: informal employment as % of total employment.

Informal employment encompasses jobs that lack social security coverage,
written contracts, or legal recognition — a defining feature of labor markets
in developing economies. High informality limits access to social protection,
suppresses tax revenues, and weakens collective bargaining.

Above 50% informality is typical of low-income countries; above 80% signals
a severely segmented labor market. The ILO Decent Work Agenda targets
progressively reducing informality.

Scoring:
    score = clip(informal_pct * 1.25, 0, 100)

    informal = 0%   -> score = 0   (fully formal)
    informal = 40%  -> score = 50
    informal = 60%  -> score = 75
    informal = 80%  -> score = 100 (capped)

Sources: ILO / WDI (SL.EMP.INSV.FE.ZS proxy; ILOSTAT EMP_2EMP_SEX_STE_INF_NB_A)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

SERIES = "EMP_INF_SEX_RT"


class InformalEmploymentShare(LayerBase):
    layer_id = "lLI"
    name = "Informal Employment Share"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'EMP_INF_SEX_RT'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no informal employment data (EMP_INF_SEX_RT)",
            }

        latest_date = rows[0]["date"]
        informal_pct = float(rows[0]["value"])

        score = float(np.clip(informal_pct * 1.25, 0.0, 100.0))

        if informal_pct >= 70:
            severity = "very high"
        elif informal_pct >= 50:
            severity = "high"
        elif informal_pct >= 25:
            severity = "moderate"
        else:
            severity = "low"

        trend_direction = "insufficient data"
        recent = sorted(rows[:10], key=lambda r: r["date"])
        if len(recent) >= 3:
            vals = np.array([float(r["value"]) for r in recent], dtype=float)
            slope = float(np.polyfit(np.arange(len(vals), dtype=float), vals, 1)[0])
            trend_direction = "rising" if slope > 0.3 else "falling" if slope < -0.3 else "stable"

        return {
            "score": round(score, 2),
            "country": country,
            "informal_employment_pct": round(informal_pct, 2),
            "severity": severity,
            "trend": trend_direction,
            "latest_date": latest_date,
            "n_obs": len(rows),
            "note": (
                "score = clip(informal_pct * 1.25, 0, 100). "
                "Series: EMP_INF_SEX_RT (ILOSTAT)."
            ),
        }
