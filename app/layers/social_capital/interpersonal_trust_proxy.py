"""Interpersonal Trust Proxy module.

Uses adult literacy rate (SE.ADT.LITR.ZS, %) as a trust proxy
(higher education -> higher social trust) and child mortality
(SH.DYN.MORT, per 1,000 live births) as an inverse trust proxy
(high mortality = breakdown of social support systems).

Score formula:
  literacy_score  = clip(100 - literacy_pct, 0, 100) [high literacy = low stress]
  mortality_score = clip(mortality / 3, 0, 100)       [high mortality = high stress; cap at 300]
  score = mean of available component scores

Sources: World Bank WDI (SH.DYN.MORT, SE.ADT.LITR.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class InterpersonalTrustProxy(LayerBase):
    layer_id = "lSC"
    name = "Interpersonal Trust Proxy"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT ds.series_id, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('SH.DYN.MORT', 'SE.ADT.LITR.ZS')
            ORDER BY ds.series_id, dp.date
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for SH.DYN.MORT or SE.ADT.LITR.ZS",
            }

        latest: dict[str, float] = {}
        series_values: dict[str, list[float]] = {}
        for r in rows:
            series_values.setdefault(r["series_id"], []).append(float(r["value"]))
        for sid, vals in series_values.items():
            latest[sid] = vals[-1]

        component_scores: list[float] = []
        literacy_score = None
        mortality_score = None

        if "SE.ADT.LITR.ZS" in latest:
            literacy_pct = latest["SE.ADT.LITR.ZS"]
            literacy_score = float(np.clip(100.0 - literacy_pct, 0.0, 100.0))
            component_scores.append(literacy_score)

        if "SH.DYN.MORT" in latest:
            mortality = latest["SH.DYN.MORT"]
            # Under-5 mortality per 1,000 live births; 300 = 100% stress
            mortality_score = float(np.clip(mortality / 3.0, 0.0, 100.0))
            component_scores.append(mortality_score)

        if not component_scores:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        score = float(np.mean(component_scores))

        return {
            "score": round(score, 1),
            "country": country,
            "adult_literacy_pct": round(latest.get("SE.ADT.LITR.ZS", float("nan")), 2),
            "child_mortality_per1000": round(latest.get("SH.DYN.MORT", float("nan")), 2),
            "literacy_stress_score": round(literacy_score, 1) if literacy_score is not None else None,
            "mortality_stress_score": round(mortality_score, 1) if mortality_score is not None else None,
            "n_components": len(component_scores),
            "note": "High score = low interpersonal trust proxy",
        }
