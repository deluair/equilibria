"""Health technology assessment (HTA) capacity index.

Measures a country's institutional capacity to evaluate and adopt health
technologies effectively. Proxied by total current health expenditure as % of GDP
(SH.XPD.CHEX.GD.ZS) reflecting investment willingness, and regulatory quality
(RQ.EST from World Bank Governance Indicators) reflecting the institutional
environment required for rigorous HTA processes.

Key references:
    Drummond, M. et al. (2008). Key principles for the improved conduct of
        health technology assessments for resource allocation decisions.
        International Journal of Technology Assessment in Health Care, 24(3), 244-258.
    Oortwijn, W. et al. (2013). Paving the way for more effective HTA in low and
        middle income countries. Health Research Policy and Systems, 11, 13.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class HtaCapacityIndex(LayerBase):
    layer_id = "lHT"
    name = "HTA Capacity Index"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Score health technology assessment capacity.

        Combines health expenditure share (investment signal) with regulatory
        quality (institutional signal). Low scores on either dimension indicate
        limited HTA capacity (high stress).

        Returns dict with score, signal, and HTA capacity metrics.
        """
        chex_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("SH.XPD.CHEX.GD.ZS", "%current health expenditure%"),
        )
        rq_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("RQ.EST", "%regulatory quality%"),
        )

        chex_values = [float(r["value"]) for r in chex_rows if r["value"] is not None]
        rq_values = [float(r["value"]) for r in rq_rows if r["value"] is not None]

        if not chex_values and not rq_values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No health expenditure or regulatory quality data in DB",
            }

        metrics: dict = {}
        component_scores: list[float] = []

        if chex_values:
            median_chex = float(np.median(chex_values))
            metrics["mean_health_exp_pct_gdp"] = round(float(np.mean(chex_values)), 2)
            metrics["median_health_exp_pct_gdp"] = round(median_chex, 2)
            metrics["n_chex_obs"] = len(chex_values)
            # Benchmark: 5% of GDP (WHO threshold) = moderate (score 50 raw)
            chex_score = float(np.clip((median_chex / 5.0) * 50.0, 0, 100))
            component_scores.append(chex_score)

        if rq_values:
            median_rq = float(np.median(rq_values))
            metrics["mean_regulatory_quality"] = round(float(np.mean(rq_values)), 3)
            metrics["median_regulatory_quality"] = round(median_rq, 3)
            metrics["n_rq_obs"] = len(rq_values)
            # RQ.EST ranges roughly -2.5 to +2.5; map to 0-100
            rq_score = float(np.clip(((median_rq + 2.5) / 5.0) * 100.0, 0, 100))
            component_scores.append(rq_score)

        capacity_score = float(np.mean(component_scores))
        # Invert: low capacity = high stress
        score = float(np.clip(100.0 - capacity_score, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": metrics,
        }
