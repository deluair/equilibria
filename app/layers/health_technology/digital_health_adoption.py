"""Digital health adoption: internet penetration relative to hospital bed capacity.

Uses internet user penetration (IT.NET.USER.ZS) as a proxy for the population
capable of accessing digital health services, relative to hospital beds per
1,000 people (SH.MED.BEDS.ZS) as a measure of physical health infrastructure.
Countries with high internet access but low bed capacity have the strongest
incentive and opportunity to adopt digital health technologies.

Key references:
    Catalani, C. et al. (2012). mHealth for HIV treatment & prevention: a
        systematic review of the literature. The Open AIDS Journal, 6, 17-41.
    Schweitzer, J. & Synowiec, C. (2012). The economics of eHealth and mHealth.
        Journal of Health Communication, 17(S1), 73-81.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class DigitalHealthAdoption(LayerBase):
    layer_id = "lHT"
    name = "Digital Health Adoption"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Score digital health adoption readiness.

        High internet penetration with low hospital beds signals a population
        that needs and can access digital health solutions. Low internet access
        signals inability to benefit from digital health (stress).

        Returns dict with score, signal, and adoption metrics.
        """
        net_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("IT.NET.USER.ZS", "%internet%users%"),
        )
        bed_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("SH.MED.BEDS.ZS", "%hospital beds%"),
        )

        net_values = [float(r["value"]) for r in net_rows if r["value"] is not None]
        bed_values = [float(r["value"]) for r in bed_rows if r["value"] is not None]

        if not net_values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No internet penetration data in DB",
            }

        median_net = float(np.median(net_values))
        metrics: dict = {
            "mean_internet_users_pct": round(float(np.mean(net_values)), 2),
            "median_internet_users_pct": round(median_net, 2),
            "n_internet_obs": len(net_values),
        }

        if bed_values:
            median_beds = float(np.median(bed_values))
            metrics["mean_hospital_beds_per1k"] = round(float(np.mean(bed_values)), 2)
            metrics["median_hospital_beds_per1k"] = round(median_beds, 2)
            metrics["n_beds_obs"] = len(bed_values)

        # Internet penetration score: low penetration = high stress
        # Benchmark: 50% internet users = moderate (score 50 raw)
        net_score = float(np.clip((median_net / 50.0) * 50.0, 0, 100))
        # Invert: low internet access -> high stress
        score = float(np.clip(100.0 - net_score, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": metrics,
        }
