"""Brain Gain Index module.

Proxies net high-skill talent flows using:
- Tertiary enrollment (SE.TER.ENRR) — domestic talent pipeline
- Net migration (SM.POP.NETM) — net demographic pull/push

Positive net migration + high tertiary enrollment = brain gain.
Score: 50 at neutral, lower = stronger gain, higher = stronger drain.

Sources: World Bank WDI
"""

from __future__ import annotations

import math

from app.layers.base import LayerBase


class BrainGainIndex(LayerBase):
    layer_id = "lKE"
    name = "Brain Gain Index"

    async def compute(self, db, **kwargs) -> dict:
        ter_code = "SE.TER.ENRR"
        ter_name = "School enrollment, tertiary"
        mig_code = "SM.POP.NETM"
        mig_name = "Net migration"

        ter_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (ter_code, f"%{ter_name}%"),
        )
        mig_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (mig_code, f"%{mig_name}%"),
        )

        ter_vals = [float(r["value"]) for r in ter_rows if r["value"] is not None] if ter_rows else []
        mig_vals = [float(r["value"]) for r in mig_rows if r["value"] is not None] if mig_rows else []

        if not ter_vals and not mig_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no brain gain data available"}

        components: dict[str, float | None] = {}
        scores: list[float] = []

        if ter_vals:
            ter = ter_vals[0]
            components["tertiary_enrollment_pct"] = round(ter, 2)
            # Higher tertiary enrollment = stronger pipeline = lower score (better)
            scores.append(max(0.0, min(100.0, (1.0 - ter / 80.0) * 100.0)))

        if mig_vals:
            net_mig = mig_vals[0]
            components["net_migration"] = net_mig
            # Positive migration = gain; scale by ±500k threshold (sigmoid)
            mig_score = max(0.0, min(100.0, 50.0 - 50.0 * math.tanh(net_mig / 500_000.0)))
            scores.append(mig_score)

        score = sum(scores) / len(scores)
        return {
            "score": round(score, 1),
            "components": components,
            "dimensions_scored": len(scores),
        }
