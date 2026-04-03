"""Health AI readiness: digital infrastructure, talent, and R&D capacity.

Assesses a country's readiness to develop and deploy AI-powered health
technologies. Combines internet user penetration (IT.NET.USER.ZS) as a proxy
for digital infrastructure and population connectivity, tertiary enrollment
(SE.TER.ENRR) as a proxy for STEM talent supply, and R&D expenditure as %
of GDP (GB.XPD.RSDV.GD.ZS) as the investment signal for AI research capacity.

Key references:
    Topol, E.J. (2019). High-performance medicine: the convergence of human
        and artificial intelligence. Nature Medicine, 25(1), 44-56.
    Obermeyer, Z. & Emanuel, E.J. (2016). Predicting the future: big data,
        machine learning, and clinical medicine. NEJM, 375(13), 1216-1219.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class HealthAiReadiness(LayerBase):
    layer_id = "lHT"
    name = "Health AI Readiness"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Score health AI readiness from internet, education, and R&D data.

        Three-component composite: digital access, tertiary talent pipeline,
        and R&D investment. Countries weak on all three face the largest gap
        in AI-driven health technology deployment.

        Returns dict with score, signal, and readiness component metrics.
        """
        net_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("IT.NET.USER.ZS", "%internet%users%"),
        )
        edu_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("SE.TER.ENRR", "%tertiary%enrollment%"),
        )
        rnd_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("GB.XPD.RSDV.GD.ZS", "%research%development%expenditure%"),
        )

        net_values = [float(r["value"]) for r in net_rows if r["value"] is not None]
        edu_values = [float(r["value"]) for r in edu_rows if r["value"] is not None]
        rnd_values = [float(r["value"]) for r in rnd_rows if r["value"] is not None]

        if not net_values and not edu_values and not rnd_values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No internet, education, or R&D data in DB",
            }

        metrics: dict = {}
        component_scores: list[float] = []

        if net_values:
            median_net = float(np.median(net_values))
            metrics["mean_internet_users_pct"] = round(float(np.mean(net_values)), 2)
            metrics["median_internet_users_pct"] = round(median_net, 2)
            metrics["n_internet_obs"] = len(net_values)
            # Benchmark: 60% internet penetration = moderate AI infrastructure (score 50 raw)
            net_score = float(np.clip((median_net / 60.0) * 50.0, 0, 100))
            component_scores.append(net_score)

        if edu_values:
            median_edu = float(np.median(edu_values))
            metrics["mean_tertiary_enrollment_pct"] = round(float(np.mean(edu_values)), 2)
            metrics["median_tertiary_enrollment_pct"] = round(median_edu, 2)
            metrics["n_edu_obs"] = len(edu_values)
            # Benchmark: 40% tertiary enrollment = moderate talent pipeline (score 50 raw)
            edu_score = float(np.clip((median_edu / 40.0) * 50.0, 0, 100))
            component_scores.append(edu_score)

        if rnd_values:
            median_rnd = float(np.median(rnd_values))
            metrics["mean_rnd_pct_gdp"] = round(float(np.mean(rnd_values)), 3)
            metrics["median_rnd_pct_gdp"] = round(median_rnd, 3)
            metrics["n_rnd_obs"] = len(rnd_values)
            # Benchmark: 2% of GDP R&D = moderate AI research capacity (score 50 raw)
            rnd_score = float(np.clip((median_rnd / 2.0) * 50.0, 0, 100))
            component_scores.append(rnd_score)

        readiness_score = float(np.mean(component_scores))
        # Invert: low readiness = high stress
        score = float(np.clip(100.0 - readiness_score, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": metrics,
        }
