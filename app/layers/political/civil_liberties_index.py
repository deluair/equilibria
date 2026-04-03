"""Civil Liberties Index module.

Civil liberties proxy via voice and accountability + rule of law composites.

Theory:
    Civil liberties encompass freedom of expression, association, and protection
    from arbitrary state action. Davenport (2007) shows that democracies have
    systematically lower repression, while Poe & Tate (1994) document that low
    voice and accountability predicts human rights violations. WGI VA.EST and
    RL.EST serve as credible cross-national proxies.

Indicators:
    - VA.EST: Voice and Accountability (WGI). Range -2.5 to 2.5. Higher = better.
    - RL.EST: Rule of Law (WGI). Range -2.5 to 2.5. Higher = better.

Score construction:
    composite_wgi = (va + rl) / 2  [range -2.5 to 2.5]
    score = clip(50 - composite_wgi * 20, 0, 100)
    Low values = high civil liberties stress.

References:
    Davenport, C. (2007). "State Repression and Political Order." ARPS 10.
    Poe, S. & Tate, N. (1994). "Repression of Human Rights." APSR 88(4).
    World Bank. (2023). Worldwide Governance Indicators.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CivilLibertiesIndex(LayerBase):
    layer_id = "l12"
    name = "Civil Liberties Index"

    async def compute(self, db, **kwargs) -> dict:
        """Estimate civil liberties stress.

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

        rl_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%RL.EST%' OR ds.name LIKE '%rule%of%law%estimate%'
                   OR ds.name LIKE '%rule%law%wgi%')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not va_rows and not rl_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no civil liberties data"}

        va_latest = 0.0
        va_detail = None
        if va_rows:
            va = np.array([float(r["value"]) for r in va_rows])
            va_latest = float(va[-1])
            va_detail = {
                "latest": round(va_latest, 4),
                "mean": round(float(np.mean(va)), 4),
                "n_obs": len(va),
                "date_range": [str(va_rows[0]["date"]), str(va_rows[-1]["date"])],
            }

        rl_latest = 0.0
        rl_detail = None
        if rl_rows:
            rl = np.array([float(r["value"]) for r in rl_rows])
            rl_latest = float(rl[-1])
            rl_detail = {
                "latest": round(rl_latest, 4),
                "mean": round(float(np.mean(rl)), 4),
                "n_obs": len(rl),
                "date_range": [str(rl_rows[0]["date"]), str(rl_rows[-1]["date"])],
            }

        composite = (va_latest + rl_latest) / 2.0
        score = float(np.clip(50 - composite * 20, 0, 100))

        result = {
            "score": round(score, 2),
            "country": country,
            "composite_wgi": round(composite, 4),
            "civil_liberties_stress": (
                "severe" if score > 70 else "elevated" if score > 45 else "low"
            ),
            "reference": "Davenport 2007; Poe & Tate 1994; WGI VA.EST + RL.EST",
        }

        if va_detail:
            result["voice_accountability"] = va_detail
        if rl_detail:
            result["rule_of_law"] = rl_detail

        return result
