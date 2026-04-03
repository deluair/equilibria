"""Platform Labor Regulation Gap module.

High gig/self-employment share paired with weak regulatory quality creates a
labor regulation gap for platform workers.

Score: higher gap = higher score (worse for workers).

Source: World Bank WDI (SL.EMP.SELF.ZS), World Bank WGI (RQ.EST inverted)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PlatformLaborRegulationGap(LayerBase):
    layer_id = "lPE"
    name = "Platform Labor Regulation Gap"

    async def compute(self, db, **kwargs) -> dict:
        code = "SL.EMP.SELF.ZS"
        name = "self-employed"
        self_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        code2 = "RQ.EST"
        name2 = "regulatory quality"
        rq_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (code2, f"%{name2}%"),
        )

        if not self_rows and not rq_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no self-employment/regulatory quality data"}

        self_vals = [float(r["value"]) for r in self_rows if r["value"] is not None]
        rq_vals = [float(r["value"]) for r in rq_rows if r["value"] is not None]

        self_mean = float(np.nanmean(self_vals)) if self_vals else None
        rq_mean = float(np.nanmean(rq_vals)) if rq_vals else None

        components, weights = [], []
        if self_mean is not None:
            components.append(float(np.clip(self_mean, 0, 100)))
            weights.append(0.5)
        if rq_mean is not None:
            # RQ.EST -2.5 to 2.5; invert so weak regulation = high gap
            rq_inverted = float(np.clip((2.5 - rq_mean) / 5.0 * 100, 0, 100))
            components.append(rq_inverted)
            weights.append(0.5)

        if not components:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no usable values"}

        total_w = sum(weights)
        score = sum(c * w for c, w in zip(components, weights)) / total_w
        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 1),
            "self_employment_pct": round(self_mean, 2) if self_mean is not None else None,
            "regulatory_quality_est": round(rq_mean, 3) if rq_mean is not None else None,
            "note": "RQ.EST inverted: weaker regulation = larger labor regulation gap.",
            "_citation": "World Bank WDI: SL.EMP.SELF.ZS; World Bank WGI: RQ.EST",
        }
