"""Healthcare market concentration analysis.

Measures supply-side concentration using hospital bed density and physician
density as proxies for provider availability. Low density relative to
cross-country norms indicates concentrated (low-competition) markets.

Key references:
    Gaynor, M. & Town, R. (2011). Competition in health care markets.
        Handbook of Health Economics, 2, 499-637.
    WHO Global Health Observatory: hospital bed density, physician density.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class HealthcareMarketConcentration(LayerBase):
    layer_id = "lHM"
    name = "Healthcare Market Concentration"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Estimate supply concentration from bed and physician density.

        Low provider density -> high market concentration -> higher score (stress).
        """
        code_beds = "SH.MED.BEDS.ZS"
        code_phys = "SH.MED.PHYS.ZS"

        beds_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code_beds, f"%{code_beds}%"),
        )
        phys_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code_phys, f"%{code_phys}%"),
        )

        if not beds_rows and not phys_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No bed or physician density data in DB",
            }

        beds_vals = [float(r["value"]) for r in beds_rows if r["value"] is not None]
        phys_vals = [float(r["value"]) for r in phys_rows if r["value"] is not None]

        if not beds_vals and not phys_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No valid bed or physician density values",
            }

        # Benchmark: WHO adequacy thresholds (beds >=3/1000, phys >=1/1000)
        beds_mean = float(np.mean(beds_vals)) if beds_vals else None
        phys_mean = float(np.mean(phys_vals)) if phys_vals else None

        # Concentration score: how far below thresholds are average values
        beds_stress = max(0.0, (3.0 - beds_mean) / 3.0) if beds_mean is not None else 0.5
        phys_stress = max(0.0, (1.0 - phys_mean) / 1.0) if phys_mean is not None else 0.5

        n_components = (1 if beds_vals else 0) + (1 if phys_vals else 0)
        score = float(np.clip(((beds_stress + phys_stress) / n_components) * 100, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "mean_beds_per_1000": round(beds_mean, 3) if beds_mean is not None else None,
                "mean_physicians_per_1000": round(phys_mean, 3) if phys_mean is not None else None,
                "beds_who_threshold": 3.0,
                "phys_who_threshold": 1.0,
                "beds_n_obs": len(beds_vals),
                "phys_n_obs": len(phys_vals),
            },
        }
