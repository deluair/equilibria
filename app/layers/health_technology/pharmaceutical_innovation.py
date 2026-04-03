"""Pharmaceutical innovation: patent-based and R&D intensity signals.

Measures pharmaceutical innovation capacity by combining resident patent
applications (IP.PAT.RESD) as a proxy for innovation output with gross R&D
expenditure as % of GDP (GB.XPD.RSDV.GD.ZS) as the investment input proxy.
Unlike the broader MedicalInnovationIndex, this module emphasizes the
pipeline between R&D investment and patentable pharmaceutical outputs.

Key references:
    DiMasi, J.A. et al. (2016). Innovation in the pharmaceutical industry:
        new estimates of R&D costs. Journal of Health Economics, 47, 20-33.
    Grabowski, H. et al. (2002). Returns on research and development for
        1990s new drug introductions. PharmacoEconomics, 20(3), 11-29.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PharmaceuticalInnovation(LayerBase):
    layer_id = "lHT"
    name = "Pharmaceutical Innovation"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Score pharmaceutical innovation from patent and R&D data.

        Assesses R&D-to-patent pipeline efficiency. Low R&D spending relative
        to GDP and low patent output signal weak pharmaceutical innovation
        capacity (high stress).

        Returns dict with score, signal, and innovation pipeline metrics.
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
        component_scores: list[float] = []

        if pat_values:
            median_pat = float(np.median(pat_values))
            metrics["mean_patent_applications"] = round(float(np.mean(pat_values)), 1)
            metrics["median_patent_applications"] = round(median_pat, 1)
            metrics["n_patent_obs"] = len(pat_values)
            # Benchmark: 5,000 patent applications = moderate pharma pipeline (score 50 raw)
            pat_score = float(np.clip((median_pat / 5_000.0) * 50.0, 0, 100))
            component_scores.append(pat_score)

        if rnd_values:
            median_rnd = float(np.median(rnd_values))
            metrics["mean_rnd_pct_gdp"] = round(float(np.mean(rnd_values)), 3)
            metrics["median_rnd_pct_gdp"] = round(median_rnd, 3)
            metrics["n_rnd_obs"] = len(rnd_values)
            # Benchmark: 1.5% of GDP for pharma-capable R&D (score 50 raw)
            rnd_score = float(np.clip((median_rnd / 1.5) * 50.0, 0, 100))
            component_scores.append(rnd_score)

        innovation_score = float(np.mean(component_scores))
        # Invert: low innovation = high stress
        score = float(np.clip(100.0 - innovation_score, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": metrics,
        }
