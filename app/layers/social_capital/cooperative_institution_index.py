"""Cooperative Institution Index module.

Captures financial inclusion and cooperative economic capacity using:
  FX.OWN.TOTL.ZS  - Account ownership at financial institution (% age 15+)
  FS.AST.PRVT.GD.ZS - Domestic credit to private sector (% of GDP)

Higher financial inclusion and credit access = stronger cooperative institutions
= lower stress.

Score formula:
  inclusion_score = clip(100 - account_pct, 0, 100)
  credit_score    = clip(100 - min(credit_pct, 200) / 200 * 100, 0, 100)
  score = mean of available component scores

Sources: World Bank WDI (FX.OWN.TOTL.ZS, FS.AST.PRVT.GD.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CooperativeInstitutionIndex(LayerBase):
    layer_id = "lSC"
    name = "Cooperative Institution Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT ds.series_id, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('FX.OWN.TOTL.ZS', 'FS.AST.PRVT.GD.ZS')
            ORDER BY ds.series_id, dp.date
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for FX.OWN.TOTL.ZS or FS.AST.PRVT.GD.ZS",
            }

        latest: dict[str, float] = {}
        series_values: dict[str, list[float]] = {}
        for r in rows:
            series_values.setdefault(r["series_id"], []).append(float(r["value"]))
        for sid, vals in series_values.items():
            latest[sid] = vals[-1]

        component_scores: list[float] = []
        inclusion_score = None
        credit_score = None

        if "FX.OWN.TOTL.ZS" in latest:
            account_pct = latest["FX.OWN.TOTL.ZS"]
            inclusion_score = float(np.clip(100.0 - account_pct, 0.0, 100.0))
            component_scores.append(inclusion_score)

        if "FS.AST.PRVT.GD.ZS" in latest:
            credit_pct = latest["FS.AST.PRVT.GD.ZS"]
            credit_score = float(np.clip(100.0 - min(credit_pct, 200.0) / 200.0 * 100.0, 0.0, 100.0))
            component_scores.append(credit_score)

        if not component_scores:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        score = float(np.mean(component_scores))

        return {
            "score": round(score, 1),
            "country": country,
            "account_ownership_pct": round(latest.get("FX.OWN.TOTL.ZS", float("nan")), 2),
            "private_credit_pct_gdp": round(latest.get("FS.AST.PRVT.GD.ZS", float("nan")), 2),
            "inclusion_stress_score": round(inclusion_score, 1) if inclusion_score is not None else None,
            "credit_stress_score": round(credit_score, 1) if credit_score is not None else None,
            "n_components": len(component_scores),
            "note": "High score = weak cooperative/financial institution capacity",
        }
