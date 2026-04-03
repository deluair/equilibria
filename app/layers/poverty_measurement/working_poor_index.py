"""Working Poor Index module.

Measures the share of workers in vulnerable employment -- own-account workers
and contributing family workers who typically lack formal employment contracts,
social protection, and decent pay (SL.EMP.VULN.ZS). A high vulnerable
employment share is strongly correlated with working poverty.

Score = clip(vulnerable_employment_share * 1.2, 0, 100).

Sources: WDI (SL.EMP.VULN.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class WorkingPoorIndex(LayerBase):
    layer_id = "lPM"
    name = "Working Poor Index"

    async def compute(self, db, **kwargs) -> dict:
        code = "SL.EMP.VULN.ZS"
        name = "vulnerable employment"

        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for SL.EMP.VULN.ZS"}

        values = [float(r["value"]) for r in rows if r["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid values"}

        latest = values[0]
        mean_val = float(np.mean(values))
        trend = float(np.polyfit(range(len(values)), values, 1)[0]) if len(values) >= 3 else 0.0

        score = float(np.clip(latest * 1.2, 0, 100))

        return {
            "score": round(score, 1),
            "vulnerable_employment_pct": round(latest, 2),
            "mean_vulnerable_pct": round(mean_val, 2),
            "trend_per_period": round(trend, 3),
            "n_obs": len(values),
            "indicator": code,
            "definition": "own-account + contributing family workers as % of total employment",
        }
