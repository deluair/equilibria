"""Counterfeit medicine risk: rule of law and regulatory quality composite.

Weak rule of law and poor regulatory quality create environments permissive to
counterfeit and substandard medicines. Both World Governance Indicators are
used (inverted) to construct a counterfeit risk score.

Key references:
    WHO (2017). A study on the public health and socioeconomic impact of
        substandard and falsified medical products. World Health Organization.
    Kaufmann, D., Kraay, A. & Mastruzzi, M. (2010). The Worldwide Governance
        Indicators. World Bank Policy Research Working Paper 5430.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CounterfeitMedicineRisk(LayerBase):
    layer_id = "lPH"
    name = "Counterfeit Medicine Risk"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Estimate counterfeit medicine risk from governance indicators.

        Uses RL.EST (rule of law estimate) and RQ.EST (regulatory quality
        estimate), both from World Governance Indicators. Both range roughly
        -2.5 to +2.5; lower values signal weaker protection against counterfeits.
        Score is inverted: poor governance -> high counterfeit risk -> high score.

        Returns dict with score, signal, and relevant metrics.
        """
        rl_code = "RL.EST"
        rl_name = "rule of law"
        rq_code = "RQ.EST"
        rq_name = "regulatory quality"

        rl_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (rl_code, f"%{rl_name}%"),
        )
        rq_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (rq_code, f"%{rq_name}%"),
        )

        if not rl_rows and not rq_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"No data for {rl_code} or {rq_code} in DB",
            }

        def _extract(rows):
            vals = [float(r["value"]) for r in rows if r["value"] is not None]
            return vals[0] if vals else None

        rl_latest = _extract(rl_rows)
        rq_latest = _extract(rq_rows)

        available = [v for v in [rl_latest, rq_latest] if v is not None]
        if not available:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "All fetched rows have NULL value",
            }

        composite = float(np.mean(available))
        # WGI range: -2.5 to +2.5. Invert and normalize to 0-100.
        # composite = -2.5 -> score = 100 (max risk); +2.5 -> score = 0
        score = float(np.clip(((composite * -1) + 2.5) / 5.0 * 100, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "rule_of_law_est_latest": round(rl_latest, 3) if rl_latest is not None else None,
                "regulatory_quality_est_latest": round(rq_latest, 3) if rq_latest is not None else None,
                "governance_composite": round(composite, 3),
                "indicators": [rl_code, rq_code],
            },
        }
