"""Medical innovation index: patent activity and health R&D intensity.

Combines domestic patent applications (IP.PAT.RESD) as a proxy for innovation
output with research and development expenditure (GB.XPD.RSDV.GD.ZS) as a proxy
for health R&D inputs. Countries with high patent activity and R&D spending
relative to GDP are better positioned to develop new health technologies.

Key references:
    Salter, A.J. & Martin, B.R. (2001). The economic benefits of publicly funded
        basic research: a critical review. Research Policy, 30(3), 509-532.
    Lanjouw, J.O. & Cockburn, I.M. (2001). New pills for poor people? Empirical
        evidence after GATT. World Development, 29(2), 265-289.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MedicalInnovationIndex(LayerBase):
    layer_id = "lHT"
    name = "Medical Innovation Index"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Score medical innovation from patent activity and R&D spending.

        Fetches resident patent applications (IP.PAT.RESD) and total R&D
        expenditure as % of GDP (GB.XPD.RSDV.GD.ZS) as health R&D proxy.
        Higher scores indicate stronger innovation capacity (lower stress).
        Score is inverted so low innovation -> high score (stress framing).

        Returns dict with score, signal, and innovation metrics.
        """
        pat_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("IP.PAT.RESD", "%patent%resident%"),
        )
        rnd_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("GB.XPD.RSDV.GD.ZS", "%research%development%expenditure%"),
        )

        pat_values = [float(r["value"]) for r in pat_rows if r["value"] is not None]
        rnd_values = [float(r["value"]) for r in rnd_rows if r["value"] is not None]

        if not pat_values and not rnd_values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No patent or R&D data in DB",
            }

        metrics: dict = {}

        if pat_values:
            metrics["mean_patent_applications"] = round(float(np.mean(pat_values)), 1)
            metrics["median_patent_applications"] = round(float(np.median(pat_values)), 1)
            metrics["n_patent_obs"] = len(pat_values)

        if rnd_values:
            metrics["mean_rnd_pct_gdp"] = round(float(np.mean(rnd_values)), 3)
            metrics["median_rnd_pct_gdp"] = round(float(np.median(rnd_values)), 3)
            metrics["n_rnd_obs"] = len(rnd_values)

        # Normalize each series 0-100 (higher = stronger innovation)
        component_scores: list[float] = []

        if pat_values:
            median_pat = float(np.median(pat_values))
            # Benchmark: 10,000 resident applications = moderate innovation (score 50)
            pat_score = float(np.clip((median_pat / 10_000.0) * 50.0, 0, 100))
            component_scores.append(pat_score)

        if rnd_values:
            median_rnd = float(np.median(rnd_values))
            # Benchmark: 2% of GDP on R&D = moderate (score 50)
            rnd_score = float(np.clip((median_rnd / 2.0) * 50.0, 0, 100))
            component_scores.append(rnd_score)

        innovation_score = float(np.mean(component_scores))
        # Invert: low innovation = high stress score
        score = float(np.clip(100.0 - innovation_score, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": metrics,
        }
