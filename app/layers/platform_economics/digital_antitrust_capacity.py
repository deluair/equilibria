"""Digital Antitrust Capacity module.

Regulatory quality and rule of law together proxy a state's capacity to enforce
digital antitrust rules against dominant platforms.

Score: higher capacity = lower score (better). Inverted so low capacity = high risk score.

Source: World Bank WGI (RQ.EST, RL.EST)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class DigitalAntitrustCapacity(LayerBase):
    layer_id = "lPE"
    name = "Digital Antitrust Capacity"

    async def compute(self, db, **kwargs) -> dict:
        code = "RQ.EST"
        name = "regulatory quality"
        rq_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        code2 = "RL.EST"
        name2 = "rule of law"
        rl_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (code2, f"%{name2}%"),
        )

        if not rq_rows and not rl_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no regulatory quality/rule of law data"}

        rq_vals = [float(r["value"]) for r in rq_rows if r["value"] is not None]
        rl_vals = [float(r["value"]) for r in rl_rows if r["value"] is not None]

        rq_mean = float(np.nanmean(rq_vals)) if rq_vals else None
        rl_mean = float(np.nanmean(rl_vals)) if rl_vals else None

        components, weights = [], []
        if rq_mean is not None:
            # Normalize -2.5..2.5 to 0..100; higher = stronger capacity
            rq_norm = float(np.clip((rq_mean + 2.5) / 5.0 * 100, 0, 100))
            components.append(rq_norm)
            weights.append(0.5)
        if rl_mean is not None:
            rl_norm = float(np.clip((rl_mean + 2.5) / 5.0 * 100, 0, 100))
            components.append(rl_norm)
            weights.append(0.5)

        if not components:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no usable values"}

        total_w = sum(weights)
        capacity = sum(c * w for c, w in zip(components, weights)) / total_w
        # Invert: low antitrust capacity = high risk score
        score = float(np.clip(100 - capacity, 0, 100))

        return {
            "score": round(score, 1),
            "regulatory_quality_est": round(rq_mean, 3) if rq_mean is not None else None,
            "rule_of_law_est": round(rl_mean, 3) if rl_mean is not None else None,
            "antitrust_capacity_index": round(capacity, 1),
            "note": "Score inverted: higher score = lower antitrust capacity = higher platform risk.",
            "_citation": "World Bank WGI: RQ.EST, RL.EST",
        }
