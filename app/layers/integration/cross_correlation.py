"""Cross-Layer Correlation Analysis.

Computes pairwise correlations between all layer scores over time.
Rolling correlations detect regime changes in co-movement patterns.
Flags unusual correlation spikes that may indicate contagion.
"""

import json
import logging
from datetime import datetime, timezone

import numpy as np
from scipy import stats

from app.layers.base import LayerBase

logger = logging.getLogger(__name__)

LAYER_IDS = ["l1", "l2", "l3", "l4", "l5"]

# Rolling window for correlation computation
DEFAULT_WINDOW = 12

# Threshold for flagging unusual correlation changes
CORR_CHANGE_THRESHOLD = 0.3

# Minimum observations for reliable correlation
MIN_OBS = 8


class CrossLayerCorrelation(LayerBase):
    layer_id = "l6"
    name = "Cross-Layer Correlation"

    async def compute(self, db, **kwargs) -> dict:
        country_iso3 = kwargs.get("country_iso3", "USA")
        window = kwargs.get("window", DEFAULT_WINDOW)
        lookback = kwargs.get("lookback", 52)  # ~1 year of weekly data

        # Fetch time series for each layer
        layer_series = await self._fetch_layer_series(
            db, country_iso3, lookback
        )

        available = [lid for lid in LAYER_IDS if lid in layer_series]
        if len(available) < 2:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "correlation_matrix": {},
                "country_iso3": country_iso3,
                "reason": f"Need at least 2 layer series, got {len(available)}",
            }

        # Align series to common dates
        aligned = self._align_series(layer_series, available)

        if aligned.shape[0] < MIN_OBS:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "correlation_matrix": {},
                "country_iso3": country_iso3,
                "reason": f"Insufficient aligned observations ({aligned.shape[0]} < {MIN_OBS})",
            }

        # Full-sample correlation matrix
        corr_matrix = self._compute_correlation_matrix(aligned, available)

        # Rolling correlations
        rolling_corrs = self._compute_rolling_correlations(
            aligned, available, window
        )

        # Detect unusual co-movements
        anomalies = self._detect_anomalies(rolling_corrs, available)

        # Average absolute correlation as a score (higher = more interconnected)
        avg_abs_corr = self._average_abs_correlation(corr_matrix, available)
        # Map to 0-100: avg_abs_corr ranges from 0 to 1
        score = avg_abs_corr * 100.0

        # Eigenvalue analysis for principal component dominance
        eigenvalues = self._eigenvalue_analysis(corr_matrix, available)

        await self._store_result(
            db, country_iso3, score, corr_matrix, anomalies
        )

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "correlation_matrix": corr_matrix,
            "rolling_correlations": rolling_corrs,
            "anomalies": anomalies,
            "eigenvalues": eigenvalues,
            "avg_absolute_correlation": round(avg_abs_corr, 4),
            "layers_analyzed": available,
            "observations": aligned.shape[0],
            "window": window,
            "country_iso3": country_iso3,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    async def _fetch_layer_series(
        self, db, country_iso3: str, lookback: int
    ) -> dict[str, list[dict]]:
        series = {}
        for lid in LAYER_IDS:
            rows = await db.fetch_all(
                """
                SELECT score, created_at FROM analysis_results
                WHERE layer = ? AND country_iso3 = ? AND score IS NOT NULL
                ORDER BY created_at DESC LIMIT ?
                """,
                (lid, country_iso3, lookback),
            )
            if rows:
                # Reverse to chronological order
                series[lid] = list(reversed(rows))
        return series

    def _align_series(
        self, layer_series: dict[str, list[dict]], available: list[str]
    ) -> np.ndarray:
        """Align series by index position (assuming same cadence).

        Returns (T x N) array where T is the common length.
        """
        min_len = min(len(layer_series[lid]) for lid in available)
        aligned = np.zeros((min_len, len(available)))
        for j, lid in enumerate(available):
            series = layer_series[lid]
            # Take the most recent min_len observations
            for i in range(min_len):
                aligned[i, j] = series[len(series) - min_len + i]["score"]
        return aligned

    def _compute_correlation_matrix(
        self, data: np.ndarray, labels: list[str]
    ) -> dict:
        """Pearson correlation matrix."""
        n = len(labels)
        matrix = {}
        for i in range(n):
            row = {}
            for j in range(n):
                if i == j:
                    row[labels[j]] = 1.0
                else:
                    r, p = stats.pearsonr(data[:, i], data[:, j])
                    row[labels[j]] = {
                        "correlation": round(float(r), 4),
                        "p_value": round(float(p), 4),
                        "significant": p < 0.05,
                    }
            matrix[labels[i]] = row
        return matrix

    def _compute_rolling_correlations(
        self, data: np.ndarray, labels: list[str], window: int
    ) -> dict:
        """Rolling pairwise correlations."""
        t, n = data.shape
        if t < window:
            return {}

        rolling = {}
        for i in range(n):
            for j in range(i + 1, n):
                pair = f"{labels[i]}_{labels[j]}"
                corrs = []
                for start in range(t - window + 1):
                    end = start + window
                    r, _ = stats.pearsonr(
                        data[start:end, i], data[start:end, j]
                    )
                    corrs.append(round(float(r), 4))
                rolling[pair] = {
                    "values": corrs,
                    "current": corrs[-1] if corrs else None,
                    "mean": round(float(np.mean(corrs)), 4),
                    "std": round(float(np.std(corrs, ddof=1)), 4) if len(corrs) > 1 else 0.0,
                }
        return rolling

    def _detect_anomalies(
        self, rolling_corrs: dict, labels: list[str]
    ) -> list[dict]:
        """Flag pairs where recent correlation deviates sharply from historical."""
        anomalies = []
        for pair, data in rolling_corrs.items():
            values = data.get("values", [])
            if len(values) < 4:
                continue

            current = values[-1]
            historical_mean = float(np.mean(values[:-1]))
            historical_std = float(np.std(values[:-1], ddof=1)) if len(values) > 2 else 0.0

            change = abs(current - historical_mean)
            if change > CORR_CHANGE_THRESHOLD:
                z_score = change / historical_std if historical_std > 0.01 else change / 0.01
                anomalies.append({
                    "pair": pair,
                    "current_correlation": current,
                    "historical_mean": round(historical_mean, 4),
                    "change": round(change, 4),
                    "z_score": round(float(z_score), 2),
                    "direction": "strengthening" if abs(current) > abs(historical_mean) else "weakening",
                })

        return sorted(anomalies, key=lambda x: x["z_score"], reverse=True)

    def _average_abs_correlation(
        self, corr_matrix: dict, labels: list[str]
    ) -> float:
        """Average absolute off-diagonal correlation."""
        values = []
        for i, li in enumerate(labels):
            for j, lj in enumerate(labels):
                if i >= j:
                    continue
                entry = corr_matrix.get(li, {}).get(lj, {})
                if isinstance(entry, dict) and "correlation" in entry:
                    values.append(abs(entry["correlation"]))
        return float(np.mean(values)) if values else 0.0

    def _eigenvalue_analysis(
        self, corr_matrix: dict, labels: list[str]
    ) -> dict:
        """Eigenvalue decomposition of correlation matrix.

        High first-eigenvalue dominance = single common factor driving all layers.
        """
        n = len(labels)
        mat = np.eye(n)
        for i in range(n):
            for j in range(n):
                if i == j:
                    continue
                entry = corr_matrix.get(labels[i], {}).get(labels[j], {})
                if isinstance(entry, dict) and "correlation" in entry:
                    mat[i, j] = entry["correlation"]

        eigenvalues = np.sort(np.linalg.eigvalsh(mat))[::-1]
        total = float(np.sum(np.maximum(eigenvalues, 0)))

        return {
            "eigenvalues": [round(float(e), 4) for e in eigenvalues],
            "variance_explained": [
                round(float(max(e, 0) / total), 4) if total > 0 else 0.0
                for e in eigenvalues
            ],
            "first_component_share": round(
                float(max(eigenvalues[0], 0) / total), 4
            ) if total > 0 else 0.0,
            "effective_dimensions": round(
                float(total ** 2 / np.sum(eigenvalues ** 2)), 2
            ) if np.sum(eigenvalues ** 2) > 0 else n,
        }

    async def _store_result(
        self, db, country_iso3: str, score: float,
        corr_matrix: dict, anomalies: list,
    ):
        await db.execute(
            """
            INSERT INTO analysis_results (analysis_type, country_iso3, layer, parameters, result, score, signal)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "cross_correlation",
                country_iso3,
                "l6",
                json.dumps({"method": "pearson_rolling"}),
                json.dumps({
                    "anomaly_count": len(anomalies),
                    "anomalies": anomalies[:5],  # top 5
                }),
                round(score, 2),
                self.classify_signal(score),
            ),
        )
