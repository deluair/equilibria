"""Resilience Composite.

Overall economic resilience index: trade diversification + reserve buffer +
governance quality + fiscal space. Weighted inverse stress composite.
Low resilience = high score (vulnerability).
"""

import json
import logging
from datetime import datetime, timezone

import numpy as np

from app.layers.base import LayerBase

logger = logging.getLogger(__name__)

INDICATORS = {
    "trade_openness": "NE.TRD.GNFS.ZS",    # Trade (% of GDP) -- proxy for diversification
    "reserve_months": "FI.RES.TOTL.MO",     # Total reserves (months of imports)
    "governance": "GE.EST",                  # Government effectiveness (WGI, -2.5 to 2.5)
    "govt_debt": "GC.DOD.TOTL.GD.ZS",       # Central govt debt (% of GDP)
}

# Each dimension's resilience score direction:
# trade_openness: higher = more resilient (lower stress)
# reserve_months: higher = more resilient
# governance: higher = more resilient
# govt_debt: lower = more resilient (more fiscal space)

NORM_BOUNDS = {
    "trade_openness": (0.0, 150.0),
    "reserve_months": (0.0, 24.0),
    "governance": (-2.5, 2.5),
    "govt_debt": (0.0, 150.0),
}

# Weight by contribution to resilience
WEIGHTS = {
    "trade_openness": 0.20,
    "reserve_months": 0.30,
    "governance": 0.25,
    "govt_debt": 0.25,
}


class ResilienceComposite(LayerBase):
    layer_id = "l6"
    name = "Resilience Composite"

    async def compute(self, db, **kwargs) -> dict:
        country_iso3 = kwargs.get("country_iso3", "USA")

        raw = await self._fetch_indicators(db, country_iso3)
        available = {k: v for k, v in raw.items() if v is not None}

        if len(available) < 2:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "country_iso3": country_iso3,
                "reason": f"Need at least 2 indicators, got {len(available)}",
            }

        # Resilience score per dimension (0 = no resilience, 100 = max resilience)
        resilience_components = {}

        if "trade_openness" in available:
            lo, hi = NORM_BOUNDS["trade_openness"]
            resilience_components["trade_openness"] = float(
                np.clip((available["trade_openness"] - lo) / (hi - lo), 0.0, 1.0) * 100.0
            )

        if "reserve_months" in available:
            lo, hi = NORM_BOUNDS["reserve_months"]
            resilience_components["reserve_months"] = float(
                np.clip((available["reserve_months"] - lo) / (hi - lo), 0.0, 1.0) * 100.0
            )

        if "governance" in available:
            lo, hi = NORM_BOUNDS["governance"]
            resilience_components["governance"] = float(
                np.clip((available["governance"] - lo) / (hi - lo), 0.0, 1.0) * 100.0
            )

        if "govt_debt" in available:
            lo, hi = NORM_BOUNDS["govt_debt"]
            # Higher debt = less fiscal space = lower resilience: invert
            resilience_components["govt_debt"] = float(
                np.clip(1.0 - (available["govt_debt"] - lo) / (hi - lo), 0.0, 1.0) * 100.0
            )

        # Weighted average resilience
        total_weight = sum(WEIGHTS[k] for k in resilience_components)
        resilience_score = sum(
            resilience_components[k] * WEIGHTS[k] / total_weight
            for k in resilience_components
        )

        # Final score: vulnerability = inverse of resilience (low resilience = high score)
        score = float(np.clip(100.0 - resilience_score, 0.0, 100.0))

        if score < 25:
            resilience_level = "high"
        elif score < 50:
            resilience_level = "moderate"
        elif score < 75:
            resilience_level = "low"
        else:
            resilience_level = "very_low"

        await self._store_result(
            db, country_iso3, score, resilience_components, resilience_score, raw
        )

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "resilience_level": resilience_level,
            "resilience_score": round(resilience_score, 2),
            "resilience_components": {k: round(v, 2) for k, v in resilience_components.items()},
            "raw_values": {k: round(v, 4) if v is not None else None for k, v in raw.items()},
            "indicators_used": list(available.keys()),
            "country_iso3": country_iso3,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "methodology": (
                "Weighted composite of trade openness, reserve adequacy, governance quality, "
                "and fiscal space. Score = 100 - resilience (low resilience = high vulnerability)."
            ),
        }

    async def _fetch_indicators(
        self, db, country_iso3: str
    ) -> dict[str, float | None]:
        result = {}
        for key, indicator_id in INDICATORS.items():
            row = await db.fetch_one(
                """
                SELECT dp.value
                FROM data_points dp
                JOIN data_series ds ON dp.series_id = ds.id
                JOIN countries c ON ds.country_id = c.id
                WHERE c.iso3 = ? AND ds.indicator_id = ?
                  AND dp.value IS NOT NULL
                ORDER BY dp.year DESC LIMIT 1
                """,
                (country_iso3, indicator_id),
            )
            result[key] = float(row["value"]) if row else None
        return result

    async def _store_result(
        self, db, country_iso3: str, score: float,
        components: dict, resilience_score: float, raw: dict,
    ):
        await db.execute(
            """
            INSERT INTO analysis_results
              (analysis_type, country_iso3, layer, parameters, result, score, signal)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "resilience_composite",
                country_iso3,
                "l6",
                json.dumps({"indicators": INDICATORS, "weights": WEIGHTS}),
                json.dumps({
                    "resilience_components": components,
                    "resilience_score": round(resilience_score, 2),
                    "raw_values": raw,
                }),
                round(score, 2),
                self.classify_signal(score),
            ),
        )
