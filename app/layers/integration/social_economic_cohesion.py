"""Social-Economic Cohesion Composite.

Combines Gini coefficient, unemployment rate, and poverty headcount into a
social fragmentation score. High composite = high social fragmentation stress.
"""

import json
import logging
from datetime import datetime, timezone

import numpy as np

from app.layers.base import LayerBase

logger = logging.getLogger(__name__)

INDICATORS = {
    "gini": "SI.POV.GINI",          # Gini index (0-100)
    "unemployment": "SL.UEM.TOTL.ZS", # Unemployment, total (% of labor force)
    "poverty": "SI.POV.DDAY",        # Poverty headcount ratio at $2.15/day (% population)
}

# Normalization bounds (approximate global ranges)
NORM_BOUNDS = {
    "gini": (20.0, 65.0),
    "unemployment": (0.0, 30.0),
    "poverty": (0.0, 80.0),
}

# All are "higher = more stress"
WEIGHTS = {
    "gini": 0.35,
    "unemployment": 0.30,
    "poverty": 0.35,
}


class SocialEconomicCohesion(LayerBase):
    layer_id = "l6"
    name = "Social-Economic Cohesion"

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

        stress_components = {}
        for k, v in available.items():
            lo, hi = NORM_BOUNDS[k]
            stress_components[k] = float(np.clip((v - lo) / (hi - lo), 0.0, 1.0) * 100.0)

        total_weight = sum(WEIGHTS[k] for k in stress_components)
        score = float(np.clip(
            sum(stress_components[k] * WEIGHTS[k] / total_weight for k in stress_components),
            0.0, 100.0,
        ))

        # Interpret fragmentation level
        if score < 25:
            fragmentation = "low"
        elif score < 50:
            fragmentation = "moderate"
        elif score < 75:
            fragmentation = "high"
        else:
            fragmentation = "severe"

        await self._store_result(db, country_iso3, score, stress_components, raw)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "fragmentation_level": fragmentation,
            "stress_components": {k: round(v, 2) for k, v in stress_components.items()},
            "raw_values": {k: round(v, 4) if v is not None else None for k, v in raw.items()},
            "indicators_used": list(available.keys()),
            "country_iso3": country_iso3,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "methodology": (
                "Weighted composite of Gini, unemployment, and extreme poverty stress. "
                "High score = high social fragmentation."
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
        self, db, country_iso3: str, score: float, components: dict, raw: dict
    ):
        await db.execute(
            """
            INSERT INTO analysis_results
              (analysis_type, country_iso3, layer, parameters, result, score, signal)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "social_economic_cohesion",
                country_iso3,
                "l6",
                json.dumps({"indicators": INDICATORS, "weights": WEIGHTS}),
                json.dumps({"stress_components": components, "raw_values": raw}),
                round(score, 2),
                self.classify_signal(score),
            ),
        )
