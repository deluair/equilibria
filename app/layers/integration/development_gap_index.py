"""Development Gap Index.

Measures multi-dimensional development gap across income, education, and health
relative to a frontier benchmark. Score = distance from frontier (higher = larger gap).
"""

import json
import logging
from datetime import datetime, timezone

import numpy as np

from app.layers.base import LayerBase

logger = logging.getLogger(__name__)

INDICATORS = {
    "gdp_per_capita": "NY.GDP.PCAP.KD",       # GDP per capita (constant 2015 USD)
    "education_spend": "SE.XPD.TOTL.GD.ZS",   # Government expenditure on education (% of GDP)
    "life_expectancy": "SP.DYN.LE00.IN",       # Life expectancy at birth (years)
}

# Frontier benchmarks (high-income / global best approximation)
FRONTIER = {
    "gdp_per_capita": 60000.0,   # USD
    "education_spend": 7.0,      # % of GDP
    "life_expectancy": 83.0,     # years
}

# Minimum plausible floor (for normalization)
FLOOR = {
    "gdp_per_capita": 300.0,
    "education_spend": 0.5,
    "life_expectancy": 45.0,
}

WEIGHTS = {
    "gdp_per_capita": 0.45,
    "education_spend": 0.25,
    "life_expectancy": 0.30,
}


class DevelopmentGapIndex(LayerBase):
    layer_id = "l6"
    name = "Development Gap Index"

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

        # Gap = how far below frontier, normalized to [0, 100]
        gap_components = {}
        for k, v in available.items():
            frontier = FRONTIER[k]
            floor = FLOOR[k]
            span = max(frontier - floor, 1.0)
            # Distance from frontier as fraction of span (0 = at frontier, 1 = at floor)
            gap = float(np.clip((frontier - v) / span, 0.0, 1.0))
            gap_components[k] = gap * 100.0

        total_weight = sum(WEIGHTS[k] for k in gap_components)
        score = float(np.clip(
            sum(gap_components[k] * WEIGHTS[k] / total_weight for k in gap_components),
            0.0, 100.0,
        ))

        await self._store_result(db, country_iso3, score, gap_components, raw)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "gap_components": {k: round(v, 2) for k, v in gap_components.items()},
            "raw_values": {k: round(v, 4) if v is not None else None for k, v in raw.items()},
            "frontier_benchmarks": FRONTIER,
            "indicators_used": list(available.keys()),
            "country_iso3": country_iso3,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "methodology": (
                "Distance from frontier benchmark across income, education, and health. "
                "Score = weighted average gap (0 = at frontier, 100 = maximum gap)."
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
                "development_gap_index",
                country_iso3,
                "l6",
                json.dumps({"indicators": INDICATORS, "frontier": FRONTIER, "weights": WEIGHTS}),
                json.dumps({"gap_components": components, "raw_values": raw}),
                round(score, 2),
                self.classify_signal(score),
            ),
        )
