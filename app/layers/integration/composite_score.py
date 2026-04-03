"""Composite Economic Analysis Score (CEAS).

Weighted average of L1-L5 layer scores producing a single 0-100 composite.
Signal classification with hysteresis to avoid flip-flopping at boundaries.
"""

import json
import logging
from datetime import datetime, timezone

import numpy as np

from app.config import LAYER_WEIGHTS, SIGNAL_LEVELS
from app.layers.base import LayerBase

logger = logging.getLogger(__name__)

LAYER_IDS = ["l1", "l2", "l3", "l4", "l5"]

# Signal thresholds (score ranges)
SIGNAL_THRESHOLDS = {
    "STABLE": (0.0, 25.0),
    "WATCH": (25.0, 50.0),
    "STRESS": (50.0, 75.0),
    "CRISIS": (75.0, 100.0),
}

# Hysteresis buffer: require crossing threshold by this much to change signal
HYSTERESIS_BUFFER = 2.0


class CompositeEconomicScore(LayerBase):
    layer_id = "l6"
    name = "Composite Economic Analysis Score"
    weight = 1.0  # meta-layer, full weight

    async def compute(self, db, **kwargs) -> dict:
        country_iso3 = kwargs.get("country_iso3", "USA")
        weights = kwargs.get("weights", LAYER_WEIGHTS)
        previous_signal = kwargs.get("previous_signal")

        # Fetch latest scores for each layer from analysis_results
        layer_scores = await self._fetch_layer_scores(db, country_iso3)

        if not layer_scores:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "ceas": None,
                "component_breakdown": {},
                "country_iso3": country_iso3,
                "methodology": "No layer scores available",
            }

        # Compute weighted average
        ceas, component_breakdown = self._compute_weighted_average(
            layer_scores, weights
        )

        # Classify signal with hysteresis
        signal = self._classify_with_hysteresis(ceas, previous_signal)

        # Compute confidence based on data coverage
        available_layers = len(layer_scores)
        coverage = available_layers / len(LAYER_IDS)

        # Store result
        await self._store_result(db, country_iso3, ceas, signal, component_breakdown)

        return {
            "score": round(ceas, 2),
            "signal": signal,
            "ceas": round(ceas, 2),
            "component_breakdown": component_breakdown,
            "weights_used": weights,
            "coverage": round(coverage, 2),
            "layers_available": available_layers,
            "layers_total": len(LAYER_IDS),
            "country_iso3": country_iso3,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "methodology": "Weighted average of L1-L5 layer scores with hysteresis signal classification",
        }

    async def _fetch_layer_scores(
        self, db, country_iso3: str
    ) -> dict[str, float]:
        """Fetch the most recent score for each layer."""
        scores = {}
        for lid in LAYER_IDS:
            row = await db.fetch_one(
                """
                SELECT score FROM analysis_results
                WHERE layer = ? AND country_iso3 = ? AND score IS NOT NULL
                ORDER BY created_at DESC LIMIT 1
                """,
                (lid, country_iso3),
            )
            if row and row["score"] is not None:
                scores[lid] = float(row["score"])
        return scores

    def _compute_weighted_average(
        self, layer_scores: dict[str, float], weights: dict[str, float]
    ) -> tuple[float, dict]:
        """Compute CEAS as weighted average, renormalizing for missing layers."""
        available_weights = {
            lid: weights.get(lid, 0.20)
            for lid in layer_scores
        }
        total_weight = sum(available_weights.values())
        if total_weight == 0:
            return 50.0, {}

        # Renormalize weights to sum to 1.0
        norm_weights = {
            lid: w / total_weight for lid, w in available_weights.items()
        }

        ceas = 0.0
        breakdown = {}
        for lid, score in layer_scores.items():
            w = norm_weights[lid]
            contribution = score * w
            ceas += contribution
            breakdown[lid] = {
                "score": round(score, 2),
                "weight": round(w, 4),
                "contribution": round(contribution, 2),
            }

        return np.clip(ceas, 0.0, 100.0), breakdown

    def _classify_with_hysteresis(
        self, score: float, previous_signal: str | None
    ) -> str:
        """Classify signal level with hysteresis to prevent flip-flopping.

        If a previous signal exists, require the score to cross the threshold
        by HYSTERESIS_BUFFER points before changing the signal.
        """
        # Simple classification without hysteresis
        base_signal = self.classify_signal(score)

        if previous_signal is None or previous_signal == "UNAVAILABLE":
            return base_signal

        if previous_signal == base_signal:
            return base_signal

        # Apply hysteresis: check if score has crossed threshold convincingly
        prev_range = SIGNAL_THRESHOLDS.get(previous_signal)
        if prev_range is None:
            return base_signal

        low, high = prev_range

        # Still within the previous range (with buffer)? Keep previous signal
        if (low - HYSTERESIS_BUFFER) <= score < (high + HYSTERESIS_BUFFER):
            return previous_signal

        return base_signal

    async def _store_result(
        self, db, country_iso3: str, ceas: float, signal: str, breakdown: dict
    ):
        """Persist composite score to analysis_results."""
        await db.execute(
            """
            INSERT INTO analysis_results (analysis_type, country_iso3, layer, parameters, result, score, signal)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "composite_score",
                country_iso3,
                "l6",
                json.dumps({"weights": dict(LAYER_WEIGHTS)}),
                json.dumps({"breakdown": breakdown}),
                round(ceas, 2),
                signal,
            ),
        )
