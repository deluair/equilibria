"""Data Monopoly Risk module.

Digital penetration combined with inverted regulatory quality as a proxy for data
monopoly risk. High internet use with weak regulation = high data monopoly risk.

Score: higher risk = higher score (worse).

Source: World Bank WDI (IT.NET.USER.ZS), World Bank WGI (RQ.EST inverted)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class DataMonopolyRisk(LayerBase):
    layer_id = "lPE"
    name = "Data Monopoly Risk"

    async def compute(self, db, **kwargs) -> dict:
        code = "IT.NET.USER.ZS"
        name = "internet users"
        net_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        code2 = "RQ.EST"
        name2 = "regulatory quality"
        rq_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (code2, f"%{name2}%"),
        )

        if not net_rows and not rq_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no internet/regulatory quality data"}

        net_vals = [float(r["value"]) for r in net_rows if r["value"] is not None]
        rq_vals = [float(r["value"]) for r in rq_rows if r["value"] is not None]

        net_mean = float(np.nanmean(net_vals)) if net_vals else None
        rq_mean = float(np.nanmean(rq_vals)) if rq_vals else None

        components, weights = [], []
        if net_mean is not None:
            components.append(float(np.clip(net_mean, 0, 100)))
            weights.append(0.5)
        if rq_mean is not None:
            # RQ.EST ranges roughly -2.5 to 2.5; invert and normalize to 0-100
            rq_norm = float(np.clip((2.5 - rq_mean) / 5.0 * 100, 0, 100))
            components.append(rq_norm)
            weights.append(0.5)

        if not components:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no usable values"}

        total_w = sum(weights)
        score = sum(c * w for c, w in zip(components, weights)) / total_w
        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 1),
            "internet_users_pct": round(net_mean, 2) if net_mean is not None else None,
            "regulatory_quality_est": round(rq_mean, 3) if rq_mean is not None else None,
            "note": "RQ.EST inverted: lower regulatory quality = higher monopoly risk.",
            "_citation": "World Bank WDI: IT.NET.USER.ZS; World Bank WGI: RQ.EST",
        }
