"""Fragility Index module.

Composite measure of state fragility using World Bank governance indicators:
political stability/no violence (PV.EST), rule of law (RL.EST), and
government effectiveness (GE.EST). Fragility rises as governance scores fall.

Score = clip((1 - governance_composite) * 50, 0, 100) where low governance = high fragility.
Governance composite is normalized from [-2.5, 2.5] range to [0, 1].

Sources: WDI (PV.EST, RL.EST, GE.EST)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class FragilityIndex(LayerBase):
    layer_id = "lCW"
    name = "Fragility Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        indicators = {
            "political_stability": "PV.EST",
            "rule_of_law": "RL.EST",
            "govt_effectiveness": "GE.EST",
        }

        scores_by_indicator: dict[str, float | None] = {}

        for name, series_id in indicators.items():
            rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                ORDER BY dp.date DESC
                LIMIT 5
                """,
                (country, series_id),
            )
            vals = [float(r["value"]) for r in rows if r["value"] is not None]
            scores_by_indicator[name] = float(np.mean(vals)) if vals else None

        available = {k: v for k, v in scores_by_indicator.items() if v is not None}
        if not available:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        # Normalize each indicator from [-2.5, 2.5] to [0, 1] then average
        normalized = [(v + 2.5) / 5.0 for v in available.values()]
        governance_composite = float(np.mean(normalized))

        # Fragility = inverse of governance quality, scaled to [0, 100]
        fragility_score = float(np.clip((1.0 - governance_composite) * 100, 0, 100))

        return {
            "score": round(fragility_score, 1),
            "country": country,
            "governance_composite_normalized": round(governance_composite, 4),
            "political_stability_est": scores_by_indicator.get("political_stability"),
            "rule_of_law_est": scores_by_indicator.get("rule_of_law"),
            "govt_effectiveness_est": scores_by_indicator.get("govt_effectiveness"),
            "indicators_available": len(available),
            "indicators": indicators,
        }
