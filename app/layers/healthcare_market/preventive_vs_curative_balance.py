"""Preventive vs curative balance analysis.

Assesses whether a health system is oriented toward prevention or curative
care by combining measles immunization coverage (a marker of preventive
investment) with hospital bed density (a marker of curative capacity).

Systems with low immunization AND high bed density may be curative-heavy;
those with high immunization AND low bed density are prevention-oriented.
Stress is highest when immunization is low regardless of bed density.

Key references:
    Frenk, J. (2010). The global health system: strengthening national
        health systems as the next step for global progress.
        PLoS Medicine, 7(1).
    World Bank WDI: SH.IMM.MEAS, SH.MED.BEDS.ZS.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PreventiveVsCurativeBalance(LayerBase):
    layer_id = "lHM"
    name = "Preventive vs Curative Balance"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Score the preventive-curative balance.

        Low immunization coverage signals under-investment in prevention.
        Stress score rises as immunization falls below the 90% WHO target.
        """
        code_imm = "SH.IMM.MEAS"
        code_beds = "SH.MED.BEDS.ZS"

        imm_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code_imm, f"%{code_imm}%"),
        )
        beds_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code_beds, f"%{code_beds}%"),
        )

        if not imm_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No measles immunization data in DB",
            }

        imm_vals = [float(r["value"]) for r in imm_rows if r["value"] is not None]
        beds_vals = [float(r["value"]) for r in beds_rows if r["value"] is not None]

        if not imm_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No valid immunization values",
            }

        # WHO target: >=90% measles coverage
        mean_imm = float(np.mean(imm_vals))
        # Stress: how far below 90% target
        imm_stress = float(np.clip((90.0 - mean_imm) / 90.0, 0, 1))

        mean_beds = float(np.mean(beds_vals)) if beds_vals else None

        score = float(np.clip(imm_stress * 100, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "mean_measles_immunization_pct": round(mean_imm, 2),
                "who_immunization_target_pct": 90.0,
                "mean_beds_per_1000": round(mean_beds, 3) if mean_beds is not None else None,
                "imm_n_obs": len(imm_vals),
                "beds_n_obs": len(beds_vals),
            },
        }
