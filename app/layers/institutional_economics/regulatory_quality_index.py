"""Regulatory Quality Index module.

Uses World Bank WGI RQ.EST (Regulatory Quality estimate), inverted to stress.
Regulatory quality captures the ability of government to formulate and implement
sound policies and regulations that permit and promote private sector development.

Poor regulatory quality imposes compliance costs, creates entry barriers, enables
rent-seeking, and deters investment (World Bank Doing Business literature).
Rescaled from WGI -2.5/2.5 to 0-100 stress score.

References:
    World Bank. (2023). Worldwide Governance Indicators.
    Djankov, S. et al. (2002). The Regulation of Entry. QJE 117(1), 1-37.
    Kaufmann, D., Kraay, A. & Mastruzzi, M. (2010). WGI 1996-2009. World Bank.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class RegulatoryQualityIndex(LayerBase):
    layer_id = "lIE"
    name = "Regulatory Quality Index"

    async def compute(self, db, **kwargs) -> dict:
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("RQ.EST", "%regulatory quality%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no regulatory quality data"}

        vals = [float(r["value"]) for r in rows]
        latest = vals[0]

        # Invert WGI RQ.EST (-2.5 to 2.5) to stress (0-1)
        stress = 1.0 - (latest + 2.5) / 5.0
        stress = max(0.0, min(1.0, stress))
        score = round(stress * 100.0, 2)

        trend_dir = None
        if len(vals) >= 3:
            trend_dir = "improving" if vals[0] > vals[-1] else "deteriorating"

        tier = (
            "strong" if latest > 1.0
            else "moderate" if latest > 0.0
            else "weak" if latest > -1.0
            else "very_weak"
        )

        result = {
            "score": score,
            "metrics": {
                "rq_est_latest": round(latest, 4),
                "stress": round(stress, 4),
                "tier": tier,
                "n_obs": len(vals),
            },
            "reference": "WB RQ.EST; Djankov et al. 2002 QJE",
        }
        if trend_dir:
            result["metrics"]["trend"] = trend_dir
        return result
