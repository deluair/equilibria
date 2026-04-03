"""Scenario Simulation (What-If Analysis).

Shock one variable (oil price, interest rate, tariff, etc.), propagate through
layers using a simple VAR-based transmission model, and show composite impact.
"""

import json
import logging
from datetime import datetime, timezone

import numpy as np
from scipy import linalg

from app.config import LAYER_WEIGHTS
from app.layers.base import LayerBase

logger = logging.getLogger(__name__)

LAYER_IDS = ["l1", "l2", "l3", "l4", "l5"]
LAYER_NAMES = {
    "l1": "Trade",
    "l2": "Macro",
    "l3": "Labor",
    "l4": "Development",
    "l5": "Agricultural",
}

# Predefined shock scenarios with expected transmission channels
PREDEFINED_SHOCKS = {
    "oil_price_spike": {
        "name": "Oil Price Spike (+50%)",
        "description": "Crude oil price increases by 50%",
        "direct_impact": {
            "l1": 8.0,   # Trade costs increase
            "l2": 12.0,  # Inflation, current account
            "l3": 3.0,   # Modest labor impact initially
            "l4": 2.0,   # Minimal direct development impact
            "l5": 10.0,  # Fertilizer/energy costs up
        },
        "magnitude": 1.0,
    },
    "interest_rate_hike": {
        "name": "Interest Rate Hike (+200bps)",
        "description": "Central bank raises rates by 200 basis points",
        "direct_impact": {
            "l1": 3.0,   # Exchange rate appreciation, trade impact
            "l2": 15.0,  # Direct macro tightening
            "l3": 5.0,   # Hiring slowdown
            "l4": 4.0,   # Investment reduction
            "l5": 2.0,   # Moderate via input financing costs
        },
        "magnitude": 1.0,
    },
    "tariff_war": {
        "name": "Tariff War (+25% across the board)",
        "description": "Broad-based tariff increases of 25%",
        "direct_impact": {
            "l1": 20.0,  # Direct trade disruption
            "l2": 8.0,   # GDP hit, inflation
            "l3": 6.0,   # Job losses in trade-exposed sectors
            "l4": 3.0,   # Supply chain reconfiguration
            "l5": 7.0,   # Agricultural trade disruption
        },
        "magnitude": 1.0,
    },
    "pandemic_shock": {
        "name": "Pandemic Shock",
        "description": "Global pandemic with mobility restrictions",
        "direct_impact": {
            "l1": 15.0,  # Supply chain disruption
            "l2": 18.0,  # GDP contraction
            "l3": 20.0,  # Massive job losses
            "l4": 10.0,  # Development reversals
            "l5": 12.0,  # Food supply chain stress
        },
        "magnitude": 1.0,
    },
    "currency_crisis": {
        "name": "Currency Crisis (-30% depreciation)",
        "description": "Sudden currency depreciation of 30%",
        "direct_impact": {
            "l1": 12.0,  # Trade competitiveness shift
            "l2": 20.0,  # Inflation, capital flight, debt burden
            "l3": 8.0,   # Real wage erosion
            "l4": 5.0,   # Institutional confidence
            "l5": 6.0,   # Import-dependent food costs
        },
        "magnitude": 1.0,
    },
}

# VAR settings
VAR_LAGS = 2
MIN_OBS = 20


class ScenarioSimulation(LayerBase):
    layer_id = "l6"
    name = "Scenario Simulation"

    async def compute(self, db, **kwargs) -> dict:
        country_iso3 = kwargs.get("country_iso3", "USA")
        scenario_id = kwargs.get("scenario_id", "oil_price_spike")
        magnitude = kwargs.get("magnitude", 1.0)
        horizon = kwargs.get("horizon", 8)  # simulation periods
        custom_shock = kwargs.get("custom_shock")  # dict of layer -> direct impact

        # Get scenario definition
        if custom_shock:
            scenario = {
                "name": "Custom Shock",
                "description": kwargs.get("description", "User-defined shock"),
                "direct_impact": custom_shock,
                "magnitude": magnitude,
            }
        elif scenario_id in PREDEFINED_SHOCKS:
            scenario = PREDEFINED_SHOCKS[scenario_id].copy()
            scenario["magnitude"] = magnitude
        else:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"Unknown scenario: {scenario_id}",
                "available_scenarios": list(PREDEFINED_SHOCKS.keys()),
            }

        # Fetch current layer scores
        current_scores = await self._fetch_current_scores(db, country_iso3)

        if len(current_scores) < 3:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "reason": f"Need at least 3 layers, got {len(current_scores)}",
            }

        # Estimate transmission matrix from historical data (if available)
        transmission = await self._estimate_transmission(db, country_iso3)

        # Apply direct shock
        direct_impact = scenario["direct_impact"]
        shock_vec = {
            lid: direct_impact.get(lid, 0.0) * magnitude
            for lid in LAYER_IDS
        }

        # Propagate shock through transmission channels
        propagated = self._propagate_shock(
            shock_vec, transmission, horizon, list(current_scores.keys())
        )

        # Compute post-shock scores
        post_shock = self._apply_shock_to_scores(current_scores, propagated)

        # Compute composite impact
        pre_composite = self._compute_composite(current_scores)
        post_composite = self._compute_composite(post_shock)
        composite_change = post_composite - pre_composite

        # Signal change
        pre_signal = self.classify_signal(pre_composite)
        post_signal = self.classify_signal(post_composite)

        # Layer-by-layer impact analysis
        impact_analysis = self._analyze_impact(
            current_scores, post_shock, propagated
        )

        score = min(max(post_composite, 0.0), 100.0)

        await self._store_result(
            db, country_iso3, score, scenario_id, scenario["name"],
            composite_change, impact_analysis,
        )

        return {
            "score": round(score, 2),
            "signal": post_signal,
            "scenario": {
                "id": scenario_id,
                "name": scenario["name"],
                "description": scenario["description"],
                "magnitude": magnitude,
            },
            "pre_shock": {
                "composite": round(pre_composite, 2),
                "signal": pre_signal,
                "layer_scores": {k: round(v, 2) for k, v in current_scores.items()},
            },
            "post_shock": {
                "composite": round(post_composite, 2),
                "signal": post_signal,
                "layer_scores": {k: round(v, 2) for k, v in post_shock.items()},
            },
            "impact": {
                "composite_change": round(composite_change, 2),
                "signal_change": pre_signal != post_signal,
                "layer_impacts": impact_analysis,
            },
            "propagation": {
                "direct": {k: round(v, 2) for k, v in shock_vec.items()},
                "total": {k: round(v, 2) for k, v in propagated.items()},
                "indirect": {
                    k: round(propagated[k] - shock_vec.get(k, 0.0), 2)
                    for k in propagated
                },
            },
            "horizon": horizon,
            "country_iso3": country_iso3,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    async def _fetch_current_scores(
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

    async def _estimate_transmission(
        self, db, country_iso3: str
    ) -> np.ndarray:
        """Estimate shock transmission matrix from historical correlations.

        Falls back to a theory-based default if insufficient data.
        """
        # Try to get historical series
        layer_series = {}
        for lid in LAYER_IDS:
            rows = await db.fetch_all(
                """
                SELECT score FROM analysis_results
                WHERE layer = ? AND country_iso3 = ? AND score IS NOT NULL
                ORDER BY created_at DESC LIMIT 52
                """,
                (lid, country_iso3),
            )
            if rows:
                layer_series[lid] = [r["score"] for r in reversed(rows)]

        available = [lid for lid in LAYER_IDS if lid in layer_series]

        if len(available) >= 3 and min(len(layer_series[lid]) for lid in available) >= MIN_OBS:
            return self._estimate_var_transmission(layer_series, available)

        # Default transmission matrix (theory-based)
        # Row i, col j: how much a unit shock in j affects i (indirect)
        # Based on economic intuition about inter-layer linkages
        return self._default_transmission_matrix()

    def _estimate_var_transmission(
        self, layer_series: dict[str, list[float]], available: list[str]
    ) -> np.ndarray:
        """Estimate transmission from VAR impulse responses."""
        k = len(LAYER_IDS)
        min_len = min(len(layer_series[lid]) for lid in available)
        data = np.zeros((min_len, len(available)))
        for j, lid in enumerate(available):
            s = layer_series[lid]
            data[:, j] = s[len(s) - min_len:]

        # First difference for stationarity
        diff = np.diff(data, axis=0)
        t, n = diff.shape

        if t <= n * VAR_LAGS + 1:
            return self._default_transmission_matrix()

        # Estimate VAR and get impulse response at horizon 1
        y = diff[VAR_LAGS:]
        x_parts = [diff[VAR_LAGS - lag: t - lag] for lag in range(1, VAR_LAGS + 1)]
        x = np.column_stack(x_parts + [np.ones(t - VAR_LAGS)])

        try:
            coeffs = linalg.solve(x.T @ x, x.T @ y, assume_a="sym")
        except linalg.LinAlgError:
            return self._default_transmission_matrix()

        # First lag coefficients give transmission intensities
        a1 = coeffs[:n].T  # n x n

        # Build full 5x5 matrix, mapping available layers
        full = np.zeros((k, k))
        lid_to_idx = {lid: i for i, lid in enumerate(LAYER_IDS)}

        for i, li in enumerate(available):
            for j, lj in enumerate(available):
                full[lid_to_idx[li], lid_to_idx[lj]] = a1[i, j]

        # Normalize: cap off-diagonal elements
        for i in range(k):
            for j in range(k):
                if i != j:
                    full[i, j] = np.clip(full[i, j], -0.5, 0.5)
            full[i, i] = 0.0  # no self-transmission (handled separately)

        return full

    def _default_transmission_matrix(self) -> np.ndarray:
        """Theory-based default transmission matrix.

        Based on standard macroeconomic linkages:
        - Trade shocks transmit strongly to macro
        - Macro shocks affect labor and development
        - Agricultural shocks feed into macro (food inflation)
        """
        # Rows: receiving layer, Cols: transmitting layer
        # l1=Trade, l2=Macro, l3=Labor, l4=Development, l5=Agricultural
        return np.array([
            [0.00, 0.15, 0.05, 0.05, 0.10],  # l1 receives from others
            [0.20, 0.00, 0.10, 0.05, 0.15],  # l2 receives (trade->macro strong)
            [0.10, 0.25, 0.00, 0.05, 0.05],  # l3 receives (macro->labor strong)
            [0.05, 0.15, 0.10, 0.00, 0.10],  # l4 receives
            [0.10, 0.10, 0.05, 0.05, 0.00],  # l5 receives
        ])

    def _propagate_shock(
        self, shock: dict[str, float], transmission: np.ndarray,
        horizon: int, available: list[str],
    ) -> dict[str, float]:
        """Propagate shock through layers over multiple periods.

        Uses the transmission matrix iteratively:
        impact_t = direct_shock + transmission @ impact_{t-1}
        """
        k = len(LAYER_IDS)
        lid_to_idx = {lid: i for i, lid in enumerate(LAYER_IDS)}

        # Initial shock vector
        impact = np.zeros(k)
        for lid, val in shock.items():
            if lid in lid_to_idx:
                impact[lid_to_idx[lid]] = val

        # Accumulate over horizon
        total_impact = impact.copy()

        for _ in range(1, horizon):
            # Indirect impact from previous period
            indirect = transmission @ impact
            impact = indirect
            total_impact += impact

            # Dampen to prevent explosive paths
            impact *= 0.7

        return {
            lid: float(total_impact[lid_to_idx[lid]])
            for lid in LAYER_IDS
            if lid in available or lid in shock
        }

    def _apply_shock_to_scores(
        self, current: dict[str, float], propagated: dict[str, float]
    ) -> dict[str, float]:
        """Apply total propagated impact to current scores."""
        post = {}
        for lid in current:
            impact = propagated.get(lid, 0.0)
            new_score = current[lid] + impact
            post[lid] = float(np.clip(new_score, 0.0, 100.0))
        return post

    def _compute_composite(self, scores: dict[str, float]) -> float:
        """Weighted average composite score."""
        total_w = sum(LAYER_WEIGHTS.get(lid, 0.20) for lid in scores)
        if total_w == 0:
            return 50.0
        return sum(
            scores[lid] * LAYER_WEIGHTS.get(lid, 0.20) / total_w
            for lid in scores
        )

    def _analyze_impact(
        self, pre: dict[str, float], post: dict[str, float],
        propagated: dict[str, float],
    ) -> list[dict]:
        """Layer-by-layer impact analysis."""
        impacts = []
        for lid in LAYER_IDS:
            if lid not in pre:
                continue
            change = post.get(lid, pre[lid]) - pre[lid]
            impacts.append({
                "layer": lid,
                "name": LAYER_NAMES.get(lid, lid),
                "pre_score": round(pre[lid], 2),
                "post_score": round(post.get(lid, pre[lid]), 2),
                "change": round(change, 2),
                "total_propagated": round(propagated.get(lid, 0.0), 2),
                "severity": (
                    "severe" if abs(change) > 15 else
                    "moderate" if abs(change) > 8 else
                    "mild" if abs(change) > 3 else
                    "minimal"
                ),
            })
        return sorted(impacts, key=lambda x: abs(x["change"]), reverse=True)

    async def _store_result(
        self, db, country_iso3: str, score: float,
        scenario_id: str, scenario_name: str,
        composite_change: float, impacts: list,
    ):
        await db.execute(
            """
            INSERT INTO analysis_results (analysis_type, country_iso3, layer, parameters, result, score, signal)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "scenario_simulation",
                country_iso3,
                "l6",
                json.dumps({"scenario": scenario_id, "name": scenario_name}),
                json.dumps({
                    "composite_change": round(composite_change, 2),
                    "top_impact": impacts[0] if impacts else None,
                }),
                round(score, 2),
                self.classify_signal(score),
            ),
        )
