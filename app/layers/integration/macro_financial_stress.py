"""Macro-Financial Stress Composite.

Combines GDP growth, domestic credit, and fiscal balance into a single
stress composite. Three stress dimensions are normalized and weighted.
"""

import json
import logging
from datetime import datetime, timezone

import numpy as np

from app.layers.base import LayerBase

logger = logging.getLogger(__name__)

# WDI indicators
INDICATORS = {
    "gdp_growth": "NY.GDP.MKTP.KD.ZG",       # GDP growth (annual %)
    "domestic_credit": "FS.AST.DOMS.GD.ZS",  # Domestic credit to private sector (% of GDP)
    "fiscal_balance": "GC.BAL.CASH.GD.ZS",   # Cash surplus/deficit (% of GDP)
}

# Stress direction: +1 = higher value -> more stress, -1 = lower value -> more stress
STRESS_DIRECTION = {
    "gdp_growth": -1,       # low growth = stress
    "domestic_credit": 1,   # very high credit = stress (credit boom)
    "fiscal_balance": -1,   # large deficit = stress
}

# Historical reference ranges for normalization (approximate global bounds)
NORM_BOUNDS = {
    "gdp_growth": (-10.0, 10.0),
    "domestic_credit": (0.0, 200.0),
    "fiscal_balance": (-15.0, 5.0),
}

# Component weights
WEIGHTS = {
    "gdp_growth": 0.40,
    "domestic_credit": 0.30,
    "fiscal_balance": 0.30,
}

MIN_YEARS = 3


class MacroFinancialStress(LayerBase):
    layer_id = "l6"
    name = "Macro-Financial Stress Composite"

    async def compute(self, db, **kwargs) -> dict:
        country_iso3 = kwargs.get("country_iso3", "USA")
        lookback = kwargs.get("lookback", 10)

        raw = await self._fetch_indicators(db, country_iso3, lookback)

        available = {k: v for k, v in raw.items() if v is not None}
        if len(available) < 2:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "country_iso3": country_iso3,
                "reason": f"Need at least 2 indicators, got {len(available)}",
            }

        stress_components = {}
        for key, value in available.items():
            stress_components[key] = self._normalize_stress(
                value, NORM_BOUNDS[key], STRESS_DIRECTION[key]
            )

        total_weight = sum(WEIGHTS[k] for k in stress_components)
        score = sum(
            stress_components[k] * WEIGHTS[k] / total_weight
            for k in stress_components
        )
        score = float(np.clip(score, 0.0, 100.0))

        await self._store_result(db, country_iso3, score, stress_components, raw)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "stress_components": {k: round(v, 2) for k, v in stress_components.items()},
            "raw_values": {k: round(v, 4) if v is not None else None for k, v in raw.items()},
            "indicators_used": list(available.keys()),
            "country_iso3": country_iso3,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "methodology": (
                "Weighted composite of GDP growth stress, domestic credit stress, "
                "and fiscal balance stress. Each dimension normalized 0-100."
            ),
        }

    async def _fetch_indicators(
        self, db, country_iso3: str, lookback: int
    ) -> dict[str, float | None]:
        """Fetch the most recent value for each indicator."""
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

    @staticmethod
    def _normalize_stress(
        value: float, bounds: tuple[float, float], direction: int
    ) -> float:
        """Normalize a value to [0, 100] stress scale.

        direction=-1: lower value -> higher stress (score closer to 100)
        direction=+1: higher value -> higher stress (score closer to 100)
        """
        lo, hi = bounds
        if hi == lo:
            return 50.0
        normalized = (value - lo) / (hi - lo)  # 0 to 1 (raw)
        normalized = float(np.clip(normalized, 0.0, 1.0))
        if direction == -1:
            # Low value = stress: invert
            return (1.0 - normalized) * 100.0
        return normalized * 100.0

    async def _store_result(
        self,
        db,
        country_iso3: str,
        score: float,
        components: dict,
        raw: dict,
    ):
        await db.execute(
            """
            INSERT INTO analysis_results
              (analysis_type, country_iso3, layer, parameters, result, score, signal)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "macro_financial_stress",
                country_iso3,
                "l6",
                json.dumps({"indicators": INDICATORS, "weights": WEIGHTS}),
                json.dumps({"stress_components": components, "raw_values": raw}),
                round(score, 2),
                self.classify_signal(score),
            ),
        )
