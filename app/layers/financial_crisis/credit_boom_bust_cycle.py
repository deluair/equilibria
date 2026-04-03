"""Credit boom-bust cycle detection module.

Uses FS.AST.PRVT.GD.ZS (private credit to GDP) to detect credit booms via
linear regression trend analysis. Rapid positive slope = boom, contraction = bust.

Score (0-100): accelerating credit boom or sharp bust both push toward CRISIS.
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase

CREDIT_CODE = "FS.AST.PRVT.GD.ZS"
CREDIT_NAME = "domestic credit private sector"


class CreditBoomBustCycle(LayerBase):
    layer_id = "lFC"
    name = "Credit Boom-Bust Cycle"

    async def compute(self, db, **kwargs) -> dict:
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (CREDIT_CODE, f"%{CREDIT_NAME}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no credit data"}

        vals = [float(r["value"]) for r in rows if r["value"] is not None]
        if len(vals) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient credit observations"}

        # Rows are DESC; reverse for chronological regression
        vals_chron = list(reversed(vals))
        x = np.arange(len(vals_chron), dtype=float)
        slope, intercept, r_value, p_value, std_err = linregress(x, vals_chron)

        level_latest = vals[0]
        level_score = float(np.clip((level_latest - 40.0) * 1.5, 0, 60))

        # Boom: slope > 3 pp/yr is concerning; bust: slope < -3 pp/yr
        boom_score = float(np.clip(slope * 10.0, 0, 60)) if slope > 0 else 0.0
        bust_score = float(np.clip(-slope * 10.0, 0, 60)) if slope < 0 else 0.0
        trend_score = max(boom_score, bust_score)

        phase = "expanding" if slope > 1.0 else "contracting" if slope < -1.0 else "stable"

        score = float(np.clip(0.40 * level_score + 0.60 * trend_score, 0, 100))

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "private_credit_gdp_pct": round(level_latest, 2),
                "annual_slope_pp": round(float(slope), 3),
                "r_squared": round(float(r_value ** 2), 4),
                "phase": phase,
                "observations": len(vals_chron),
                "level_score": round(level_score, 2),
                "trend_score": round(trend_score, 2),
            },
        }
