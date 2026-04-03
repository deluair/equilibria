"""Telemedicine readiness: broadband infrastructure and physician availability.

Telemedicine viability depends on both digital connectivity and health workforce
capacity. Uses fixed broadband subscriptions per 100 people (IT.NET.BBND.P2) as
the infrastructure proxy and physicians per 1,000 people (SH.MED.PHYS.ZS) as
the clinical supply proxy. Countries with broadband access and adequate physicians
are best positioned to deploy effective telemedicine at scale.

Key references:
    Ekeland, A.G. et al. (2010). Effectiveness of telemedicine: a systematic
        review of reviews. Global Health Action, 3(1), 5247.
    WHO (2019). WHO Guideline: Recommendations on digital interventions for
        health system strengthening. World Health Organization.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class TelemedicineReadiness(LayerBase):
    layer_id = "lHT"
    name = "Telemedicine Readiness"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Score telemedicine readiness from broadband and physician data.

        Low broadband penetration signals infrastructure gap; low physician
        density signals workforce gap. Both constrain telemedicine deployment.
        Score reflects readiness gap (high score = low readiness = stress).

        Returns dict with score, signal, and readiness metrics.
        """
        bb_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("IT.NET.BBND.P2", "%broadband%subscriptions%"),
        )
        phys_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("SH.MED.PHYS.ZS", "%physicians%"),
        )

        bb_values = [float(r["value"]) for r in bb_rows if r["value"] is not None]
        phys_values = [float(r["value"]) for r in phys_rows if r["value"] is not None]

        if not bb_values and not phys_values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No broadband or physician data in DB",
            }

        metrics: dict = {}
        component_scores: list[float] = []

        if bb_values:
            median_bb = float(np.median(bb_values))
            metrics["mean_broadband_per100"] = round(float(np.mean(bb_values)), 2)
            metrics["median_broadband_per100"] = round(median_bb, 2)
            metrics["n_broadband_obs"] = len(bb_values)
            # Benchmark: 30 subscriptions/100 = moderate connectivity (score 50 raw)
            bb_score = float(np.clip((median_bb / 30.0) * 50.0, 0, 100))
            component_scores.append(bb_score)

        if phys_values:
            median_phys = float(np.median(phys_values))
            metrics["mean_physicians_per1k"] = round(float(np.mean(phys_values)), 3)
            metrics["median_physicians_per1k"] = round(median_phys, 3)
            metrics["n_physicians_obs"] = len(phys_values)
            # Benchmark: 1 physician per 1,000 = WHO basic threshold (score 50 raw)
            phys_score = float(np.clip((median_phys / 1.0) * 50.0, 0, 100))
            component_scores.append(phys_score)

        readiness_score = float(np.mean(component_scores))
        # Invert: low readiness = high stress
        score = float(np.clip(100.0 - readiness_score, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": metrics,
        }
