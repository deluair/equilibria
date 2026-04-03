"""Political-Economic Stability Joint Signal.

Triple-negative detector: political instability + recession + inflation
simultaneously = crisis convergence. Score reflects joint severity.
"""

import json
import logging
from datetime import datetime, timezone

import numpy as np

from app.layers.base import LayerBase

logger = logging.getLogger(__name__)

INDICATORS = {
    "political_stability": "PV.EST",             # Political stability (WGI, -2.5 to 2.5)
    "gdp_growth": "NY.GDP.MKTP.KD.ZG",           # GDP growth (annual %)
    "inflation": "FP.CPI.TOTL.ZG",              # Inflation, consumer prices (annual %)
}

# Thresholds for "negative" classification
THRESHOLDS = {
    "political_stability": -0.5,   # below -0.5 = instability
    "gdp_growth": 0.0,             # below 0 = recession
    "inflation": 10.0,             # above 10% = high inflation stress
}

# Normalization bounds
NORM_BOUNDS = {
    "political_stability": (-2.5, 2.5),  # WGI scale
    "gdp_growth": (-10.0, 10.0),
    "inflation": (0.0, 50.0),
}

WEIGHTS = {
    "political_stability": 0.35,
    "gdp_growth": 0.35,
    "inflation": 0.30,
}


class PoliticalEconomicStability(LayerBase):
    layer_id = "l6"
    name = "Political-Economic Stability"

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

        # Normalize each to 0-100 stress
        stress_components = {}
        negatives = {}

        if "political_stability" in available:
            lo, hi = NORM_BOUNDS["political_stability"]
            v = available["political_stability"]
            # Low stability = high stress: invert
            stress_components["political_stability"] = float(
                np.clip((hi - v) / (hi - lo), 0.0, 1.0) * 100.0
            )
            negatives["political_stability"] = v < THRESHOLDS["political_stability"]

        if "gdp_growth" in available:
            lo, hi = NORM_BOUNDS["gdp_growth"]
            v = available["gdp_growth"]
            # Negative growth = stress
            stress_components["gdp_growth"] = float(
                np.clip((0.0 - v + 10.0) / 20.0, 0.0, 1.0) * 100.0
            )
            negatives["gdp_growth"] = v < THRESHOLDS["gdp_growth"]

        if "inflation" in available:
            lo, hi = NORM_BOUNDS["inflation"]
            v = available["inflation"]
            stress_components["inflation"] = float(np.clip(v / hi, 0.0, 1.0) * 100.0)
            negatives["inflation"] = v > THRESHOLDS["inflation"]

        total_weight = sum(WEIGHTS[k] for k in stress_components)
        base_score = sum(
            stress_components[k] * WEIGHTS[k] / total_weight
            for k in stress_components
        )

        # Triple-negative amplifier: all three negative = crisis convergence multiplier
        triple_negative = all(negatives.get(k, False) for k in ["political_stability", "gdp_growth", "inflation"])
        if triple_negative and len(negatives) == 3:
            base_score = base_score * 1.25  # 25% amplification on triple-negative

        score = float(np.clip(base_score, 0.0, 100.0))
        negative_count = sum(1 for v in negatives.values() if v)

        await self._store_result(
            db, country_iso3, score, stress_components, raw, triple_negative, negative_count
        )

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "triple_negative_convergence": triple_negative,
            "negative_signals": negative_count,
            "stress_components": {k: round(v, 2) for k, v in stress_components.items()},
            "raw_values": {k: round(v, 4) if v is not None else None for k, v in raw.items()},
            "indicators_used": list(available.keys()),
            "country_iso3": country_iso3,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "methodology": (
                "Joint political-economic stress. Triple-negative (instability + recession + "
                "high inflation) triggers 25% crisis convergence amplifier."
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
        components: dict, raw: dict, triple_neg: bool, neg_count: int,
    ):
        await db.execute(
            """
            INSERT INTO analysis_results
              (analysis_type, country_iso3, layer, parameters, result, score, signal)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "political_economic_stability",
                country_iso3,
                "l6",
                json.dumps({"indicators": INDICATORS, "thresholds": THRESHOLDS}),
                json.dumps({
                    "stress_components": components,
                    "raw_values": raw,
                    "triple_negative": triple_neg,
                    "negative_count": neg_count,
                }),
                round(score, 2),
                self.classify_signal(score),
            ),
        )
