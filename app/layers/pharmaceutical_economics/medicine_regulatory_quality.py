"""Medicine regulatory quality: governance composite from WGI estimates.

Regulatory quality (RQ.EST) and rule of law (RL.EST) together capture
the institutional environment for pharmaceutical regulation. Strong
regulatory quality enables effective drug approval, pharmacovigilance,
and market surveillance.

Key references:
    Kaufmann, D., Kraay, A. & Mastruzzi, M. (2010). The Worldwide Governance
        Indicators. World Bank Policy Research Working Paper 5430.
    Reich, M.R. (1994). Bangladesh pharmaceutical policy and politics.
        Health Policy and Planning, 9(2), 130-143.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MedicineRegulatoryQuality(LayerBase):
    layer_id = "lPH"
    name = "Medicine Regulatory Quality"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Assess medicine regulatory quality from WGI composite.

        Uses RQ.EST (regulatory quality) and RL.EST (rule of law), both from
        the World Governance Indicators. Both range -2.5 to +2.5. Higher
        composite indicates stronger regulatory environment for medicines.
        Score is inverted: poor regulation -> high score (stress).

        Returns dict with score, signal, and relevant metrics.
        """
        rq_code = "RQ.EST"
        rq_name = "regulatory quality"
        rl_code = "RL.EST"
        rl_name = "rule of law"

        rq_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (rq_code, f"%{rq_name}%"),
        )
        rl_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (rl_code, f"%{rl_name}%"),
        )

        if not rq_rows and not rl_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"No data for {rq_code} or {rl_code} in DB",
            }

        def _extract(rows):
            vals = [float(r["value"]) for r in rows if r["value"] is not None]
            return vals[0] if vals else None

        rq_latest = _extract(rq_rows)
        rl_latest = _extract(rl_rows)

        available = [v for v in [rq_latest, rl_latest] if v is not None]
        if not available:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "All fetched rows have NULL value",
            }

        composite = float(np.mean(available))
        # WGI range -2.5 to +2.5. Invert: poor regulation -> high score.
        score = float(np.clip(((composite * -1) + 2.5) / 5.0 * 100, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "regulatory_quality_est_latest": round(rq_latest, 3) if rq_latest is not None else None,
                "rule_of_law_est_latest": round(rl_latest, 3) if rl_latest is not None else None,
                "governance_composite": round(composite, 3),
                "indicators": [rq_code, rl_code],
                "scale": "-2.5 (worst) to +2.5 (best)",
            },
        }
