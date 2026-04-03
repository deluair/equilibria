"""Health data infrastructure: broadband connectivity and governance quality.

Effective health data systems require both physical connectivity to transmit
data and institutional quality to govern its collection and use. Uses fixed
broadband subscriptions per 100 people (IT.NET.BBND.P2) as the connectivity
proxy and government effectiveness (GE.EST from World Bank Governance
Indicators) as the institutional capacity proxy for health data governance.

Key references:
    Kruk, M.E. et al. (2018). Mortality due to low-quality health systems in
        the universal health coverage era: a systematic analysis of amenable
        deaths in 137 countries. The Lancet, 392(10160), 2203-2212.
    Wyber, R. et al. (2015). Big data in global health: improving health in
        low- and middle-income countries. Bulletin of the WHO, 93(3), 203-208.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class HealthDataInfrastructure(LayerBase):
    layer_id = "lHT"
    name = "Health Data Infrastructure"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Score health data infrastructure from broadband and governance data.

        Low broadband penetration limits data transmission capacity; weak
        government effectiveness limits data governance quality. Both degrade
        health information systems (high stress).

        Returns dict with score, signal, and infrastructure metrics.
        """
        bb_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("IT.NET.BBND.P2", "%broadband%subscriptions%"),
        )
        ge_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("GE.EST", "%government effectiveness%"),
        )

        bb_values = [float(r["value"]) for r in bb_rows if r["value"] is not None]
        ge_values = [float(r["value"]) for r in ge_rows if r["value"] is not None]

        if not bb_values and not ge_values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No broadband or government effectiveness data in DB",
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

        if ge_values:
            median_ge = float(np.median(ge_values))
            metrics["mean_govt_effectiveness"] = round(float(np.mean(ge_values)), 3)
            metrics["median_govt_effectiveness"] = round(median_ge, 3)
            metrics["n_ge_obs"] = len(ge_values)
            # GE.EST ranges roughly -2.5 to +2.5; map to 0-100
            ge_score = float(np.clip(((median_ge + 2.5) / 5.0) * 100.0, 0, 100))
            component_scores.append(ge_score)

        infrastructure_score = float(np.mean(component_scores))
        # Invert: low infrastructure = high stress
        score = float(np.clip(100.0 - infrastructure_score, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": metrics,
        }
