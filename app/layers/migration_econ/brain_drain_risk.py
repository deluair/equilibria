"""Brain Drain Risk module.

Estimates high-skill emigration risk using tertiary enrollment rates
combined with political instability as a proxy for the push factors
that drive educated workers to emigrate.

High tertiary enrollment signals a growing skilled workforce. When
paired with political instability (negative PV.EST), the conditions
favor brain drain: investment in human capital leaves the country.

Score reflects combined risk: education investment wasted by instability.

Sources: WDI (SE.TER.ENRR, PV.EST)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class BrainDrainRisk(LayerBase):
    layer_id = "lME"
    name = "Brain Drain Risk"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        edu_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SE.TER.ENRR'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        stab_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'PV.EST'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not edu_rows and not stab_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        edu_vals = [float(r["value"]) for r in edu_rows if r["value"] is not None]
        stab_vals = [float(r["value"]) for r in stab_rows if r["value"] is not None]

        edu_latest = float(np.mean(edu_vals)) if edu_vals else 30.0
        stab_latest = float(np.mean(stab_vals)) if stab_vals else 0.0

        # Education component: tertiary enrollment as % (normalized 0-100 -> 0-50 contribution)
        edu_score = float(np.clip(edu_latest / 2, 0, 50))

        # Instability component: PV.EST ranges roughly -2.5 to +2.5
        # Negative (unstable) drives brain drain; clip to 0-50 contribution
        instab_raw = max(0.0, -stab_latest)  # flip sign: instability is positive
        instab_score = float(np.clip(instab_raw * 20, 0, 50))

        score = edu_score + instab_score

        return {
            "score": round(score, 1),
            "country": country,
            "tertiary_enrollment_pct": round(edu_latest, 2),
            "political_stability_est": round(stab_latest, 4),
            "components": {
                "education_pressure": round(edu_score, 2),
                "instability_pressure": round(instab_score, 2),
            },
            "interpretation": (
                "high brain drain risk" if score > 65
                else "moderate risk" if score > 40
                else "low risk"
            ),
        }
