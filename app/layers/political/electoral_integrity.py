"""Electoral Integrity module.

Electoral process proxy via voice and accountability trend (VA.EST).

Theory:
    Norris (2014) defines electoral integrity as the extent to which elections
    meet international standards. In the absence of direct electoral integrity
    survey data, WGI VA.EST captures pluralism, civil liberties, and political
    participation -- the institutional conditions that determine whether elections
    translate preferences into outcomes. Sustained low VA signals structural
    constraints on electoral competition.

Indicator:
    - VA.EST: Voice and Accountability (WGI). Range -2.5 to 2.5. Higher = better.

Score construction:
    score = clip(50 - va_latest * 20, 0, 100)
    Sustained low VA = electoral integrity concern.

References:
    Norris, P. (2014). Why Electoral Integrity Matters. Cambridge UP.
    Lindberg, S. (2006). Democracy and Elections in Africa. Johns Hopkins UP.
    World Bank. (2023). Worldwide Governance Indicators.
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class ElectoralIntegrity(LayerBase):
    layer_id = "l12"
    name = "Electoral Integrity"

    async def compute(self, db, **kwargs) -> dict:
        """Estimate electoral integrity stress.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 code (default BGD)
        """
        country = kwargs.get("country_iso3", "BGD")

        va_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%VA.EST%' OR ds.name LIKE '%voice%accountability%'
                   OR ds.name LIKE '%voice%and%accountability%')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not va_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no voice/accountability data"}

        va = np.array([float(r["value"]) for r in va_rows])
        va_latest = float(va[-1])

        score = float(np.clip(50 - va_latest * 20, 0, 100))

        trend = None
        if len(va) >= 3:
            t = np.arange(len(va), dtype=float)
            slope, _, r_val, p_val, _ = stats.linregress(t, va)
            trend = {
                "slope_per_year": round(float(slope), 5),
                "direction": "improving" if slope > 0 else "declining",
                "r_squared": round(float(r_val ** 2), 4),
                "p_value": round(float(p_val), 4),
            }

        result = {
            "score": round(score, 2),
            "country": country,
            "va_latest": round(va_latest, 4),
            "va_mean": round(float(np.mean(va)), 4),
            "electoral_stress": (
                "severe" if score > 65 else "elevated" if score > 35 else "low"
            ),
            "n_obs": len(va),
            "date_range": [str(va_rows[0]["date"]), str(va_rows[-1]["date"])],
            "reference": "Norris 2014; Lindberg 2006; WGI VA.EST",
        }

        if trend:
            result["trend"] = trend

        return result
