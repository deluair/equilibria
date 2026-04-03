"""Health workforce market analysis.

Evaluates the adequacy of the physician workforce using WHO minimum
staffing thresholds. Physician density below 1 per 1,000 population
(WHO threshold for basic health services) signals a severely constrained
workforce market with likely rationing and market power for existing providers.

Key references:
    WHO (2006). World Health Report: Working Together for Health. Geneva.
    World Bank WDI: SH.MED.PHYS.ZS (physicians per 1,000 population).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class HealthWorkforceMarket(LayerBase):
    layer_id = "lHM"
    name = "Health Workforce Market"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Score health workforce market adequacy from physician density.

        Below WHO threshold of 1 physician per 1,000 population -> high stress.
        Stress scales proportionally with shortfall.
        """
        code = "SH.MED.PHYS.ZS"

        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{code}%"),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No physician density data in DB",
            }

        vals = [float(r["value"]) for r in rows if r["value"] is not None]
        if not vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No valid physician density values",
            }

        mean_phys = float(np.mean(vals))
        # WHO basic threshold: 1 physician per 1,000
        who_threshold = 1.0
        # Stress: shortfall relative to threshold, capped at 100
        stress = float(np.clip(max(0.0, who_threshold - mean_phys) / who_threshold, 0, 1))
        score = float(np.clip(stress * 100, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "mean_physicians_per_1000": round(mean_phys, 3),
                "who_threshold_per_1000": who_threshold,
                "shortfall_per_1000": round(max(0.0, who_threshold - mean_phys), 3),
                "n_obs": len(vals),
            },
        }
