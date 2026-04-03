"""Healthcare access inequality analysis.

Combines income inequality (Gini) with out-of-pocket burden to assess
whether healthcare access is distributed inequitably. High OOP spending
in a high-inequality country signals that poorer households face
disproportionate financial barriers to care.

Key references:
    Wagstaff, A. & van Doorslaer, E. (2000). Income inequality and health:
        what does the literature tell us? Annual Review of Public Health, 21.
    World Bank WDI: SI.POV.GINI, SH.XPD.OOPC.CH.ZS.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class HealthcareAccessInequality(LayerBase):
    layer_id = "lHM"
    name = "Healthcare Access Inequality"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Score healthcare access inequality from Gini and OOP burden.

        High Gini + high OOP -> severe access inequality -> high stress score.
        """
        code_gini = "SI.POV.GINI"
        code_oop = "SH.XPD.OOPC.CH.ZS"

        gini_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code_gini, f"%{code_gini}%"),
        )
        oop_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code_oop, f"%{code_oop}%"),
        )

        if not gini_rows and not oop_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No Gini or OOP data in DB",
            }

        gini_vals = [float(r["value"]) for r in gini_rows if r["value"] is not None]
        oop_vals = [float(r["value"]) for r in oop_rows if r["value"] is not None]

        if not gini_vals and not oop_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No valid Gini or OOP values",
            }

        # Normalize Gini (0-100 scale) to [0,1]: 0=perfect equality, 1=extreme
        gini_norm = float(np.clip(np.mean(gini_vals) / 100.0, 0, 1)) if gini_vals else 0.5
        # OOP component: 0-100%
        oop_norm = float(np.clip(np.mean(oop_vals) / 100.0, 0, 1)) if oop_vals else 0.5

        n_components = (1 if gini_vals else 0) + (1 if oop_vals else 0)
        composite = (gini_norm + oop_norm) / n_components
        score = float(np.clip(composite * 100, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "mean_gini": round(float(np.mean(gini_vals)), 2) if gini_vals else None,
                "mean_oop_pct_che": round(float(np.mean(oop_vals)), 2) if oop_vals else None,
                "gini_n_obs": len(gini_vals),
                "oop_n_obs": len(oop_vals),
            },
        }
