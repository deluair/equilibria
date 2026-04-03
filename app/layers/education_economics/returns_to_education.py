"""Mincer returns to schooling years.

Estimates the Mincerian earnings function:
    ln(w) = alpha + rho * S + beta_1 * X + beta_2 * X^2 + epsilon

where S is years of schooling, X is potential experience, and rho is the
private rate of return to an additional year of education.

References:
    Mincer, J. (1974). Schooling, Experience, and Earnings. NBER.
    Psacharopoulos, G. & Patrinos, H.A. (2004). Returns to investment in
        education: a further update. Education Economics, 12(2), 111-134.

Score: rho < 5% -> STRESS (underinvestment signal), 5-10% -> WATCH,
10-15% -> STABLE, > 15% -> strong returns (elevated private incentive).
"""

from __future__ import annotations

import math

from app.layers.base import LayerBase


class ReturnsToEducation(LayerBase):
    layer_id = "lED"
    name = "Returns to Education (Mincer)"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.value, dp.date, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'mincer_returns'
            ORDER BY dp.date DESC
            LIMIT 50
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no Mincer returns data"}

        values = [row["value"] for row in rows if row["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid return values"}

        rho = values[0]  # most recent return rate (percent)
        latest_year = rows[0]["date"][:4] if rows[0]["date"] else None

        # Score: 0=low returns (underinvestment), 100=crisis (inequality signal)
        # Moderate returns 8-12% are optimal per Psacharopoulos
        if rho < 5:
            score = 70.0  # underinvestment: STRESS
        elif rho < 8:
            score = 45.0  # below optimum: WATCH
        elif rho <= 12:
            score = 20.0  # healthy range: STABLE
        elif rho <= 15:
            score = 40.0  # high private return, inequality concern
        else:
            score = 65.0  # very high, signals severe education scarcity

        return {
            "score": round(score, 2),
            "country": country,
            "return_to_schooling_pct": round(rho, 3),
            "latest_year": latest_year,
            "n_obs": len(values),
            "interpretation": "annual % wage gain per additional year of schooling",
        }
