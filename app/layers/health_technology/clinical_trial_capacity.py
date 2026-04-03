"""Clinical trial capacity: physician workforce and STEM talent pipeline.

Clinical trial capacity depends on both the medical workforce to conduct trials
and the scientific talent to design and analyze them. Uses physicians per 1,000
people (SH.MED.PHYS.ZS) as the clinical capacity proxy and tertiary school
enrollment (SE.TER.ENRR) as a STEM talent pipeline proxy. Countries with
adequate physicians and educated workforces can sustain rigorous clinical
research programs.

Key references:
    Bhatt, A. (2011). Evolution of clinical research: a history before and
        beyond James Lind. Perspectives in Clinical Research, 2(4), 127-131.
    Glickman, S.W. et al. (2009). Ethical and scientific implications of
        the globalization of clinical research. NEJM, 360(8), 816-823.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ClinicalTrialCapacity(LayerBase):
    layer_id = "lHT"
    name = "Clinical Trial Capacity"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Score clinical trial capacity from physician and education data.

        Low physician density and low tertiary enrollment both constrain
        clinical research capacity (high stress). Strong clinical trial
        ecosystems require both medical and scientific human capital.

        Returns dict with score, signal, and capacity metrics.
        """
        phys_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("SH.MED.PHYS.ZS", "%physicians%"),
        )
        edu_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("SE.TER.ENRR", "%tertiary%enrollment%"),
        )

        phys_values = [float(r["value"]) for r in phys_rows if r["value"] is not None]
        edu_values = [float(r["value"]) for r in edu_rows if r["value"] is not None]

        if not phys_values and not edu_values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No physician or tertiary enrollment data in DB",
            }

        metrics: dict = {}
        component_scores: list[float] = []

        if phys_values:
            median_phys = float(np.median(phys_values))
            metrics["mean_physicians_per1k"] = round(float(np.mean(phys_values)), 3)
            metrics["median_physicians_per1k"] = round(median_phys, 3)
            metrics["n_physicians_obs"] = len(phys_values)
            # Benchmark: 2 physicians/1,000 = moderate clinical capacity (score 50 raw)
            phys_score = float(np.clip((median_phys / 2.0) * 50.0, 0, 100))
            component_scores.append(phys_score)

        if edu_values:
            median_edu = float(np.median(edu_values))
            metrics["mean_tertiary_enrollment_pct"] = round(float(np.mean(edu_values)), 2)
            metrics["median_tertiary_enrollment_pct"] = round(median_edu, 2)
            metrics["n_edu_obs"] = len(edu_values)
            # Benchmark: 40% tertiary enrollment = moderate STEM pipeline (score 50 raw)
            edu_score = float(np.clip((median_edu / 40.0) * 50.0, 0, 100))
            component_scores.append(edu_score)

        capacity_score = float(np.mean(component_scores))
        # Invert: low capacity = high stress
        score = float(np.clip(100.0 - capacity_score, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": metrics,
        }
