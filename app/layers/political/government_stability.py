"""Government Stability module.

Government effectiveness trend + political stability composite.

Theory:
    Rose (2014) shows government stability depends on administrative capacity
    (GE.EST) and the absence of political violence (PV.EST). When both decline
    simultaneously, governments face a dual legitimacy-capacity crisis that
    increases policy uncertainty and investment risk. The composite weights
    effectiveness and stability equally, following ICRG's government stability
    component methodology.

Indicators:
    - GE.EST: Government Effectiveness (WGI). Range -2.5 to 2.5. Higher = better.
    - PV.EST: Political Stability and Absence of Violence (WGI). Range -2.5 to 2.5.

Score construction:
    ge_stress = clip(0.5 - ge_latest * 0.2, 0, 1)
    pv_stress = clip(0.5 - pv_latest * 0.2, 0, 1)
    score = clip((ge_stress * 0.5 + pv_stress * 0.5) * 100, 0, 100)
    Both declining = government instability.

References:
    Rose, R. (2014). "Evaluating Democratic Governance." Democratization 21(7).
    PRS Group. (2023). International Country Risk Guide.
    World Bank. (2023). Worldwide Governance Indicators.
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class GovernmentStability(LayerBase):
    layer_id = "l12"
    name = "Government Stability"

    async def compute(self, db, **kwargs) -> dict:
        """Estimate government stability composite.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 code (default BGD)
        """
        country = kwargs.get("country_iso3", "BGD")

        ge_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%GE.EST%' OR ds.name LIKE '%government%effectiveness%estimate%'
                   OR ds.name LIKE '%government%effectiveness%wgi%')
            ORDER BY dp.date
            """,
            (country,),
        )

        pv_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%PV.EST%' OR ds.name LIKE '%political%stability%absence%violence%'
                   OR ds.name LIKE '%political%stability%no%violence%')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not ge_rows and not pv_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no government stability data"}

        ge_latest = 0.0
        ge_stress = 0.5
        ge_detail = None
        if ge_rows:
            ge = np.array([float(r["value"]) for r in ge_rows])
            ge_latest = float(ge[-1])
            ge_stress = float(np.clip(0.5 - ge_latest * 0.2, 0, 1))
            trend = None
            if len(ge) >= 3:
                t = np.arange(len(ge), dtype=float)
                slope, _, r_val, p_val, _ = stats.linregress(t, ge)
                trend = {
                    "slope_per_year": round(float(slope), 5),
                    "direction": "improving" if slope > 0 else "declining",
                    "r_squared": round(float(r_val ** 2), 4),
                }
            ge_detail = {
                "latest": round(ge_latest, 4),
                "mean": round(float(np.mean(ge)), 4),
                "n_obs": len(ge),
                "date_range": [str(ge_rows[0]["date"]), str(ge_rows[-1]["date"])],
            }
            if trend:
                ge_detail["trend"] = trend

        pv_latest = 0.0
        pv_stress = 0.5
        pv_detail = None
        if pv_rows:
            pv = np.array([float(r["value"]) for r in pv_rows])
            pv_latest = float(pv[-1])
            pv_stress = float(np.clip(0.5 - pv_latest * 0.2, 0, 1))
            trend = None
            if len(pv) >= 3:
                t = np.arange(len(pv), dtype=float)
                slope, _, r_val, p_val, _ = stats.linregress(t, pv)
                trend = {
                    "slope_per_year": round(float(slope), 5),
                    "direction": "improving" if slope > 0 else "declining",
                    "r_squared": round(float(r_val ** 2), 4),
                }
            pv_detail = {
                "latest": round(pv_latest, 4),
                "mean": round(float(np.mean(pv)), 4),
                "n_obs": len(pv),
                "date_range": [str(pv_rows[0]["date"]), str(pv_rows[-1]["date"])],
            }
            if trend:
                pv_detail["trend"] = trend

        score = float(np.clip(
            (ge_stress * 0.5 + pv_stress * 0.5) * 100,
            0, 100,
        ))

        result = {
            "score": round(score, 2),
            "country": country,
            "score_components": {
                "effectiveness_stress": round(ge_stress * 0.5 * 100, 2),
                "stability_stress": round(pv_stress * 0.5 * 100, 2),
            },
            "stability_tier": (
                "unstable" if score > 65 else "at_risk" if score > 35 else "stable"
            ),
            "reference": "Rose 2014; ICRG; WGI GE.EST + PV.EST",
        }

        if ge_detail:
            result["government_effectiveness"] = ge_detail
        if pv_detail:
            result["political_stability"] = pv_detail

        return result
