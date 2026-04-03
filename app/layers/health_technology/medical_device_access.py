"""Medical device access: hospital infrastructure and health investment.

Proxies medical device access through hospital bed availability (SH.MED.BEDS.ZS)
as a measure of physical health infrastructure, combined with current health
expenditure as % of GDP (SH.XPD.CHEX.GD.ZS) as an investment signal. Countries
with adequate beds and sufficient health spending are more likely to have
accessible medical devices including diagnostic and therapeutic equipment.

Key references:
    WHO (2011). Medical devices: managing the mismatch. An outcome of the
        priority medical devices project. World Health Organization.
    Malkin, R.A. (2007). Design of health care technologies for the developing
        world. Annual Review of Biomedical Engineering, 9, 567-587.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MedicalDeviceAccess(LayerBase):
    layer_id = "lHT"
    name = "Medical Device Access"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Score medical device access from bed availability and health spending.

        Low hospital bed density combined with low health expenditure signals
        poor device access (high stress). High bed density with adequate
        spending indicates better device availability.

        Returns dict with score, signal, and access metrics.
        """
        bed_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("SH.MED.BEDS.ZS", "%hospital beds%"),
        )
        chex_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("SH.XPD.CHEX.GD.ZS", "%current health expenditure%"),
        )

        bed_values = [float(r["value"]) for r in bed_rows if r["value"] is not None]
        chex_values = [float(r["value"]) for r in chex_rows if r["value"] is not None]

        if not bed_values and not chex_values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No hospital bed or health expenditure data in DB",
            }

        metrics: dict = {}
        component_scores: list[float] = []

        if bed_values:
            median_beds = float(np.median(bed_values))
            metrics["mean_beds_per1k"] = round(float(np.mean(bed_values)), 2)
            metrics["median_beds_per1k"] = round(median_beds, 2)
            metrics["n_beds_obs"] = len(bed_values)
            # Benchmark: 2.5 beds/1,000 = WHO minimum for basic services (score 50 raw)
            bed_score = float(np.clip((median_beds / 2.5) * 50.0, 0, 100))
            component_scores.append(bed_score)

        if chex_values:
            median_chex = float(np.median(chex_values))
            metrics["mean_health_exp_pct_gdp"] = round(float(np.mean(chex_values)), 2)
            metrics["median_health_exp_pct_gdp"] = round(median_chex, 2)
            metrics["n_chex_obs"] = len(chex_values)
            # Benchmark: 5% of GDP (WHO) = moderate (score 50 raw)
            chex_score = float(np.clip((median_chex / 5.0) * 50.0, 0, 100))
            component_scores.append(chex_score)

        access_score = float(np.mean(component_scores))
        # Invert: low access = high stress
        score = float(np.clip(100.0 - access_score, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": metrics,
        }
