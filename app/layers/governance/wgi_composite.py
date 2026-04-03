"""WGI Composite module.

World Governance Indicators composite across all 6 dimensions (World Bank).

Dimensions:
  VA.EST  Voice and Accountability
  PV.EST  Political Stability and Absence of Violence
  GE.EST  Government Effectiveness
  RQ.EST  Regulatory Quality
  RL.EST  Rule of Law
  CC.EST  Control of Corruption

Each dimension is on a [-2.5, +2.5] scale. Higher values = better governance.

Composite = simple average of all available dimensions.
Score = clip(50 - composite * 20, 0, 100).
  composite = +2.5 -> score = 0  (best governance, no stress)
  composite =  0.0 -> score = 50 (average governance)
  composite = -2.5 -> score = 100 (worst governance, crisis)

Sources: World Bank WDI (VA.EST, PV.EST, GE.EST, RQ.EST, RL.EST, CC.EST)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

WGI_INDICATORS = ["VA.EST", "PV.EST", "GE.EST", "RQ.EST", "RL.EST", "CC.EST"]
WGI_LABELS = {
    "VA.EST": "Voice and Accountability",
    "PV.EST": "Political Stability",
    "GE.EST": "Government Effectiveness",
    "RQ.EST": "Regulatory Quality",
    "RL.EST": "Rule of Law",
    "CC.EST": "Control of Corruption",
}


class WGIComposite(LayerBase):
    layer_id = "lGV"
    name = "WGI Composite"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT ds.series_id, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('VA.EST','PV.EST','GE.EST','RQ.EST','RL.EST','CC.EST')
            ORDER BY ds.series_id, dp.date
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        # Collect latest value per indicator
        latest: dict[str, float] = {}
        all_dates: list[str] = []
        series_values: dict[str, list[float]] = {}
        for r in rows:
            sid = r["series_id"]
            series_values.setdefault(sid, []).append(float(r["value"]))
            all_dates.append(r["date"])

        for sid, vals in series_values.items():
            latest[sid] = vals[-1]

        if not latest:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        values = list(latest.values())
        mean_wgi = float(np.mean(values))

        score = float(np.clip(50.0 - mean_wgi * 20.0, 0.0, 100.0))

        dimensions = {
            WGI_LABELS.get(k, k): round(v, 4) for k, v in latest.items()
        }

        return {
            "score": round(score, 1),
            "country": country,
            "wgi_composite": round(mean_wgi, 4),
            "n_dimensions": len(latest),
            "dimensions": dimensions,
            "interpretation": (
                "high governance stress" if score >= 65
                else "moderate governance concerns" if score >= 40
                else "adequate governance"
            ),
            "note": "WGI scale: -2.5 (worst) to +2.5 (best)",
        }
