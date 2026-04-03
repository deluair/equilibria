"""Cultural Heritage Value module.

Proxies cultural heritage preservation capacity via a composite of:
  - Government effectiveness / rule of law (proxy via NE.TRD.GNFS.ZS openness
    as institutional quality signal; direct governance indicators used if available)
  - Literacy rate (SE.ADT.LITR.ZS) as a proxy for cultural capital and
    heritage management workforce capacity

Strong institutions and high literacy support preservation of cultural sites
and authentic tourism experiences.

Score: 0 (strong heritage preservation capacity) to 100 (weak capacity).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CulturalHeritageValue(LayerBase):
    layer_id = "lTO"
    name = "Cultural Heritage Value"

    async def compute(self, db, **kwargs) -> dict:
        literacy_code = "SE.ADT.LITR.ZS"
        openness_code = "NE.TRD.GNFS.ZS"

        literacy_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (literacy_code, "%literacy rate%"),
        )

        openness_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (openness_code, "%trade in goods and services%"),
        )

        literacy_vals = [float(r["value"]) for r in literacy_rows if r["value"] is not None]
        openness_vals = [float(r["value"]) for r in openness_rows if r["value"] is not None]

        if not literacy_vals and not openness_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for SE.ADT.LITR.ZS or NE.TRD.GNFS.ZS for cultural heritage proxy",
            }

        components = []

        literacy_score = None
        if literacy_vals:
            latest_lit = literacy_vals[0]
            # Low literacy = weak heritage capacity = high score
            literacy_score = float(np.clip(100 - latest_lit, 0, 100))
            components.append(literacy_score)

        openness_score = None
        if openness_vals:
            latest_open = openness_vals[0]
            # Higher openness = better institutions proxy = lower score
            openness_score = float(np.clip(100 - latest_open * 0.5, 0, 100))
            components.append(openness_score)

        score = float(np.mean(components))

        return {
            "score": round(score, 1),
            "literacy_rate_pct": round(literacy_vals[0], 2) if literacy_vals else None,
            "literacy_gap_score": round(literacy_score, 1) if literacy_score is not None else None,
            "trade_openness_pct_gdp": round(openness_vals[0], 2) if openness_vals else None,
            "institutional_score": round(openness_score, 1) if openness_score is not None else None,
            "n_literacy_obs": len(literacy_vals),
            "n_openness_obs": len(openness_vals),
            "methodology": "avg(literacy_gap, openness_proxy); low literacy + low openness = high score",
        }
