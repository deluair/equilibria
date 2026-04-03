"""Regime Stability module.

Political regime persistence: political stability variance + voice variance.

Theory:
    Regime instability arises when political stability (PV.EST) is persistently
    negative and voice and accountability (VA.EST) exhibits high variance,
    signalling contested legitimacy. Sustained negative political stability
    combined with volatile voice indicators predicts regime transition risk.

Indicators:
    - PV.EST: Political Stability and Absence of Violence/Terrorism (WGI).
      Range -2.5 to 2.5. Higher = more stable.
    - VA.EST: Voice and Accountability (WGI). Range -2.5 to 2.5. Higher = better.

Score construction:
    stability_component = clip(0.5 - pv_mean * 0.2, 0, 1)  [0=stable, 1=unstable]
    voice_variance_component = clip(va_variance * 10, 0, 1)  [0=stable, 1=volatile]
    raw = (stability_component * 0.6 + voice_variance_component * 0.4) * 100
    score = clip(raw, 0, 100)  [0=no stress, 100=crisis]

References:
    Norris, P. (2008). Driving Democracy. Cambridge UP.
    World Bank. (2023). Worldwide Governance Indicators.
    Gurr, T. R. (1970). Why Men Rebel. Princeton UP.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class RegimeStability(LayerBase):
    layer_id = "l12"
    name = "Regime Stability"

    async def compute(self, db, **kwargs) -> dict:
        """Estimate political regime persistence.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 code (default BGD)
        """
        country = kwargs.get("country_iso3", "BGD")

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

        if not pv_rows and not va_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no regime stability data"}

        pv_mean = -0.5
        pv_latest = None
        stability_component = 0.5
        pv_detail = None

        if pv_rows:
            pv = np.array([float(r["value"]) for r in pv_rows])
            pv_mean = float(np.mean(pv))
            pv_latest = float(pv[-1])
            stability_component = float(np.clip(0.5 - pv_mean * 0.2, 0, 1))
            pv_detail = {
                "mean": round(pv_mean, 4),
                "latest": round(pv_latest, 4),
                "min": round(float(np.min(pv)), 4),
                "n_obs": len(pv),
                "date_range": [str(pv_rows[0]["date"]), str(pv_rows[-1]["date"])],
            }

        va_variance = 0.04
        voice_variance_component = 0.5
        va_detail = None

        if va_rows:
            va = np.array([float(r["value"]) for r in va_rows])
            va_variance = float(np.var(va))
            voice_variance_component = float(np.clip(va_variance * 10, 0, 1))
            va_detail = {
                "latest": round(float(va[-1]), 4),
                "mean": round(float(np.mean(va)), 4),
                "variance": round(va_variance, 6),
                "n_obs": len(va),
                "date_range": [str(va_rows[0]["date"]), str(va_rows[-1]["date"])],
            }

        score = float(np.clip(
            (stability_component * 0.6 + voice_variance_component * 0.4) * 100,
            0, 100,
        ))

        result = {
            "score": round(score, 2),
            "country": country,
            "score_components": {
                "stability_stress": round(stability_component * 0.6 * 100, 2),
                "voice_variance_stress": round(voice_variance_component * 0.4 * 100, 2),
            },
            "regime_risk": (
                "high" if score > 65 else "moderate" if score > 35 else "low"
            ),
            "reference": "Norris 2008; WGI PV.EST + VA.EST",
        }

        if pv_detail:
            result["political_stability"] = pv_detail
        if va_detail:
            result["voice_accountability"] = va_detail

        return result
