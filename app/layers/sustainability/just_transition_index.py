"""Just Transition Index: equity of the green transition relative to inequality.

A just energy transition requires that the benefits of decarbonization are
distributed equitably. Countries with high renewable electricity penetration but
also high income inequality represent a transition that leaves behind the poorest,
producing a high score (unjust). Countries with low renewables AND low inequality
score low because inequality is not the driver of their transition shortfall.

Methodology:
    renewables = latest EG.ELC.RNEW.ZS (%, 0-100)
    gini       = latest SI.POV.GINI   (0-100)

    score = (gini / 100) * (1 - renewables / 100) * 100

    Interpretation:
        High gini + low renewables  -> moderate score (no transition, but unjust)
        High gini + high renewables -> high score (transition proceeds but inequitably)
        Low gini  + high renewables -> low score (best case: clean and equitable)

References:
    Sovacool, B.K. et al. (2019). "Decarbonization and its discontents:
        a critical energy justice perspective on four low-carbon transitions."
        Climatic Change, 155(4), 581-619.
    Newell, P. & Mulvaney, D. (2013). "The political economy of the 'just
        transition'." Geographical Journal, 179(2), 132-140.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class JustTransitionIndex(LayerBase):
    layer_id = "lSU"
    name = "Just Transition Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('EG.ELC.RNEW.ZS', 'SI.POV.GINI')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 2:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient renewable/inequality data"}

        series: dict[str, dict[str, float]] = {}
        for r in rows:
            sid = r["series_id"]
            yr = r["date"][:4] if isinstance(r["date"], str) else str(r["date"])
            series.setdefault(sid, {})[yr] = float(r["value"])

        renewables_vals = series.get("EG.ELC.RNEW.ZS", {})
        gini_vals = series.get("SI.POV.GINI", {})

        if not renewables_vals or not gini_vals:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "one or both required series missing"}

        renewables = float(renewables_vals[max(renewables_vals.keys())])
        gini = float(gini_vals[max(gini_vals.keys())])

        score = float(np.clip((gini / 100) * (1 - renewables / 100) * 100, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "renewable_share_pct": round(renewables, 2),
            "gini_coefficient": round(gini, 2),
            "justice_interpretation": (
                "equitable_green_transition" if score < 20 else
                "moderate_justice_concerns" if score < 40 else
                "unjust_transition_risk"
            ),
        }
