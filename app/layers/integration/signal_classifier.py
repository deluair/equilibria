"""Signal Classifier.

Multi-dimensional signal classification considering level, trend, and volatility
of the composite score. Hysteresis logic prevents rapid state transitions.
"""

import json
import logging
from datetime import datetime, timezone

import numpy as np

from app.layers.base import LayerBase

logger = logging.getLogger(__name__)

# Minimum history length for trend/volatility calculation
MIN_HISTORY = 5

# Trend window (number of recent observations)
TREND_WINDOW = 12

# Volatility threshold multiplier (stdev above this = high volatility)
VOL_THRESHOLD = 1.5

# Signal transition matrix: allowed transitions (prevent multi-step jumps)
ALLOWED_TRANSITIONS = {
    "STABLE": {"STABLE", "WATCH"},
    "WATCH": {"STABLE", "WATCH", "STRESS"},
    "STRESS": {"WATCH", "STRESS", "CRISIS"},
    "CRISIS": {"STRESS", "CRISIS"},
}

# Persistence requirement: must be in new state for N periods before confirming
PERSISTENCE_PERIODS = 3


class SignalClassifier(LayerBase):
    layer_id = "l6"
    name = "Signal Classifier"

    async def compute(self, db, **kwargs) -> dict:
        country_iso3 = kwargs.get("country_iso3", "USA")
        window = kwargs.get("window", TREND_WINDOW)

        # Fetch composite score history
        history = await self._fetch_score_history(db, country_iso3, window * 2)

        if len(history) < MIN_HISTORY:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "classification": {
                    "level_signal": "UNAVAILABLE",
                    "trend_signal": "UNKNOWN",
                    "volatility_signal": "UNKNOWN",
                },
                "country_iso3": country_iso3,
                "reason": f"Insufficient history ({len(history)} < {MIN_HISTORY})",
            }

        scores = np.array([h["score"] for h in history])
        current = scores[-1]
        recent = scores[-window:] if len(scores) >= window else scores

        # Level classification
        level_signal = self.classify_signal(current)

        # Trend analysis (linear regression slope on recent scores)
        trend_slope, trend_signal = self._classify_trend(recent)

        # Volatility analysis
        volatility, vol_signal = self._classify_volatility(recent)

        # Multi-dimensional signal combining level + trend + volatility
        composite_signal = self._multi_dim_signal(
            level_signal, trend_signal, vol_signal
        )

        # Apply transition constraints
        previous_signal = await self._get_previous_signal(db, country_iso3)
        final_signal = self._apply_transition_constraint(
            composite_signal, previous_signal
        )

        # Check persistence
        persistence = self._check_persistence(history, final_signal)

        # Store
        await self._store_classification(
            db, country_iso3, final_signal, current,
            level_signal, trend_signal, vol_signal, trend_slope, volatility,
        )

        return {
            "score": round(current, 2),
            "signal": final_signal,
            "classification": {
                "level_signal": level_signal,
                "trend_signal": trend_signal,
                "volatility_signal": vol_signal,
                "composite_signal": composite_signal,
                "final_signal": final_signal,
            },
            "metrics": {
                "current_score": round(current, 2),
                "trend_slope": round(trend_slope, 4),
                "volatility": round(volatility, 4),
                "persistence_periods": persistence,
            },
            "transition": {
                "previous": previous_signal,
                "proposed": composite_signal,
                "final": final_signal,
                "constrained": composite_signal != final_signal,
            },
            "country_iso3": country_iso3,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    async def _fetch_score_history(
        self, db, country_iso3: str, limit: int
    ) -> list[dict]:
        return await db.fetch_all(
            """
            SELECT score, signal, created_at FROM analysis_results
            WHERE analysis_type = 'composite_score' AND country_iso3 = ?
              AND score IS NOT NULL
            ORDER BY created_at DESC LIMIT ?
            """,
            (country_iso3, limit),
        )

    def _classify_trend(self, scores: np.ndarray) -> tuple[float, str]:
        """Linear trend on recent scores. Positive slope = deteriorating."""
        n = len(scores)
        if n < 2:
            return 0.0, "FLAT"

        x = np.arange(n, dtype=float)
        # OLS slope
        x_mean = x.mean()
        y_mean = scores.mean()
        slope = np.sum((x - x_mean) * (scores - y_mean)) / max(
            np.sum((x - x_mean) ** 2), 1e-10
        )

        # Annualize: assume weekly observations
        if abs(slope) < 0.5:
            return slope, "FLAT"
        elif slope > 0:
            return slope, "DETERIORATING"
        else:
            return slope, "IMPROVING"

    def _classify_volatility(self, scores: np.ndarray) -> tuple[float, str]:
        """Standard deviation of recent scores relative to their mean."""
        if len(scores) < 3:
            return 0.0, "LOW"

        vol = float(np.std(scores, ddof=1))
        mean = float(np.mean(scores))
        # Coefficient of variation (avoid div by zero)
        cv = vol / max(abs(mean), 1.0)

        if cv > 0.3:
            return vol, "HIGH"
        elif cv > 0.15:
            return vol, "MODERATE"
        else:
            return vol, "LOW"

    def _multi_dim_signal(
        self, level: str, trend: str, volatility: str
    ) -> str:
        """Combine level, trend, and volatility into a single signal.

        Upgrade (worsen) signal if trend is deteriorating or volatility is high.
        Downgrade (improve) only if level is good AND trend is improving.
        """
        signal_order = ["STABLE", "WATCH", "STRESS", "CRISIS"]
        idx = signal_order.index(level) if level in signal_order else 1

        # Trend adjustment
        if trend == "DETERIORATING":
            idx = min(idx + 1, 3)
        elif trend == "IMPROVING" and volatility != "HIGH":
            idx = max(idx - 1, 0)

        # Volatility adjustment: high volatility never allows STABLE
        if volatility == "HIGH" and idx == 0:
            idx = 1

        return signal_order[idx]

    def _apply_transition_constraint(
        self, proposed: str, previous: str | None
    ) -> str:
        """Prevent multi-step signal jumps (e.g., STABLE -> CRISIS)."""
        if previous is None or previous == "UNAVAILABLE":
            return proposed

        allowed = ALLOWED_TRANSITIONS.get(previous, {proposed})
        if proposed in allowed:
            return proposed

        # Move one step toward the proposed signal
        signal_order = ["STABLE", "WATCH", "STRESS", "CRISIS"]
        prev_idx = signal_order.index(previous) if previous in signal_order else 1
        prop_idx = signal_order.index(proposed) if proposed in signal_order else 1

        if prop_idx > prev_idx:
            return signal_order[prev_idx + 1]
        else:
            return signal_order[prev_idx - 1]

    def _check_persistence(
        self, history: list[dict], signal: str
    ) -> int:
        """Count consecutive periods the signal has been at this level."""
        count = 0
        for h in history:
            if h.get("signal") == signal:
                count += 1
            else:
                break
        return count

    async def _get_previous_signal(self, db, country_iso3: str) -> str | None:
        row = await db.fetch_one(
            """
            SELECT signal FROM analysis_results
            WHERE analysis_type = 'signal_classification' AND country_iso3 = ?
            ORDER BY created_at DESC LIMIT 1
            """,
            (country_iso3,),
        )
        return row["signal"] if row else None

    async def _store_classification(
        self, db, country_iso3: str, signal: str, score: float,
        level_signal: str, trend_signal: str, vol_signal: str,
        trend_slope: float, volatility: float,
    ):
        await db.execute(
            """
            INSERT INTO analysis_results (analysis_type, country_iso3, layer, parameters, result, score, signal)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "signal_classification",
                country_iso3,
                "l6",
                json.dumps({
                    "level_signal": level_signal,
                    "trend_signal": trend_signal,
                    "volatility_signal": vol_signal,
                }),
                json.dumps({
                    "trend_slope": round(trend_slope, 4),
                    "volatility": round(volatility, 4),
                }),
                round(score, 2),
                signal,
            ),
        )
