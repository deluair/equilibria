"""Layer Attribution via Shapley Values.

Determines how much each analytical layer (L1-L5) contributes to the composite
economic score. Uses exact Shapley values for fair attribution, since we have
only 5 players (2^5 = 32 coalitions, computationally trivial).
"""

import json
import logging
from datetime import datetime, timezone
from itertools import combinations

from app.config import LAYER_WEIGHTS
from app.layers.base import LayerBase

logger = logging.getLogger(__name__)

LAYER_IDS = ["l1", "l2", "l3", "l4", "l5"]


class LayerAttribution(LayerBase):
    layer_id = "l6"
    name = "Layer Attribution"

    async def compute(self, db, **kwargs) -> dict:
        country_iso3 = kwargs.get("country_iso3", "USA")
        weights = kwargs.get("weights", LAYER_WEIGHTS)
        baseline = kwargs.get("baseline", 50.0)  # neutral score

        # Fetch current layer scores
        layer_scores = await self._fetch_layer_scores(db, country_iso3)

        if len(layer_scores) < 2:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "shapley_values": {},
                "country_iso3": country_iso3,
                "reason": f"Need at least 2 layers, got {len(layer_scores)}",
            }

        # Compute Shapley values
        shapley = self._compute_shapley_values(layer_scores, weights, baseline)

        # Compute composite for reference
        total_weight = sum(weights.get(lid, 0.20) for lid in layer_scores)
        composite = sum(
            layer_scores[lid] * weights.get(lid, 0.20) / total_weight
            for lid in layer_scores
        )

        # Rank layers by absolute contribution
        ranked = sorted(shapley.items(), key=lambda x: abs(x[1]), reverse=True)

        # Identify primary drivers
        drivers_up = [(lid, v) for lid, v in ranked if v > 0.5]
        drivers_down = [(lid, v) for lid, v in ranked if v < -0.5]

        # Marginal contributions (simpler: what if we drop each layer?)
        marginal = self._compute_marginal(layer_scores, weights, composite)

        await self._store_attribution(
            db, country_iso3, composite, shapley, marginal
        )

        return {
            "score": round(composite, 2),
            "signal": self.classify_signal(composite),
            "shapley_values": {k: round(v, 4) for k, v in shapley.items()},
            "marginal_contributions": {
                k: round(v, 4) for k, v in marginal.items()
            },
            "ranking": [
                {"layer": lid, "shapley": round(v, 4)} for lid, v in ranked
            ],
            "drivers_up": [
                {"layer": lid, "contribution": round(v, 4)}
                for lid, v in drivers_up
            ],
            "drivers_down": [
                {"layer": lid, "contribution": round(v, 4)}
                for lid, v in drivers_down
            ],
            "baseline": baseline,
            "composite": round(composite, 2),
            "country_iso3": country_iso3,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    async def _fetch_layer_scores(
        self, db, country_iso3: str
    ) -> dict[str, float]:
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

    def _compute_shapley_values(
        self,
        layer_scores: dict[str, float],
        weights: dict[str, float],
        baseline: float,
    ) -> dict[str, float]:
        """Exact Shapley values for layer attribution.

        For N players, Shapley value of player i:
        phi_i = sum over S not containing i:
            |S|!(N-|S|-1)!/N! * [v(S union {i}) - v(S)]

        v(S) = weighted average of scores in coalition S (renormalized weights).
        v({}) = baseline.
        """
        players = list(layer_scores.keys())
        n = len(players)
        shapley = {p: 0.0 for p in players}

        # Precompute factorials
        fact = [1] * (n + 1)
        for i in range(1, n + 1):
            fact[i] = fact[i - 1] * i

        for player in players:
            others = [p for p in players if p != player]

            for size in range(0, n):
                # All subsets of 'others' of this size
                for subset in combinations(others, size):
                    coalition_without = set(subset)
                    coalition_with = coalition_without | {player}

                    v_without = self._coalition_value(
                        coalition_without, layer_scores, weights, baseline
                    )
                    v_with = self._coalition_value(
                        coalition_with, layer_scores, weights, baseline
                    )

                    marginal = v_with - v_without
                    coeff = fact[size] * fact[n - size - 1] / fact[n]
                    shapley[player] += coeff * marginal

        return shapley

    def _coalition_value(
        self,
        coalition: set,
        layer_scores: dict[str, float],
        weights: dict[str, float],
        baseline: float,
    ) -> float:
        """Value function: weighted average of scores in the coalition.

        Empty coalition returns the baseline (neutral) value.
        """
        if not coalition:
            return baseline

        total_w = sum(weights.get(lid, 0.20) for lid in coalition)
        if total_w == 0:
            return baseline

        return sum(
            layer_scores[lid] * weights.get(lid, 0.20) / total_w
            for lid in coalition
        )

    def _compute_marginal(
        self,
        layer_scores: dict[str, float],
        weights: dict[str, float],
        full_composite: float,
    ) -> dict[str, float]:
        """Leave-one-out marginal contribution of each layer."""
        marginal = {}
        for lid in layer_scores:
            reduced = {k: v for k, v in layer_scores.items() if k != lid}
            if not reduced:
                marginal[lid] = full_composite
                continue

            total_w = sum(weights.get(k, 0.20) for k in reduced)
            reduced_composite = sum(
                reduced[k] * weights.get(k, 0.20) / total_w for k in reduced
            )
            marginal[lid] = full_composite - reduced_composite

        return marginal

    async def _store_attribution(
        self, db, country_iso3: str, composite: float,
        shapley: dict[str, float], marginal: dict[str, float],
    ):
        await db.execute(
            """
            INSERT INTO analysis_results (analysis_type, country_iso3, layer, parameters, result, score, signal)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "layer_attribution",
                country_iso3,
                "l6",
                json.dumps({"method": "exact_shapley"}),
                json.dumps({
                    "shapley": {k: round(v, 4) for k, v in shapley.items()},
                    "marginal": {k: round(v, 4) for k, v in marginal.items()},
                }),
                round(composite, 2),
                self.classify_signal(composite),
            ),
        )
