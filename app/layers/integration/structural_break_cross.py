"""Cross-Layer Structural Break Detection.

Detects simultaneous structural breaks across multiple layers.
Uses CUSUM-based break detection on individual series and a joint test
for synchronous breaks. Identifies regime periods.
"""

import json
import logging
from datetime import datetime, timezone

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase

logger = logging.getLogger(__name__)

LAYER_IDS = ["l1", "l2", "l3", "l4", "l5"]

# CUSUM significance level
CUSUM_ALPHA = 0.05

# Minimum segment length (fraction of series)
MIN_SEGMENT_FRAC = 0.15

# Minimum observations for break detection
MIN_OBS = 15

# Threshold: fraction of layers with breaks at same point to call it a joint break
JOINT_BREAK_THRESHOLD = 0.6


class CrossLayerBreak(LayerBase):
    layer_id = "l6"
    name = "Cross-Layer Structural Break"

    async def compute(self, db, **kwargs) -> dict:
        country_iso3 = kwargs.get("country_iso3", "USA")
        lookback = kwargs.get("lookback", 104)
        alpha = kwargs.get("alpha", CUSUM_ALPHA)

        # Fetch layer series
        layer_series = await self._fetch_layer_series(
            db, country_iso3, lookback
        )

        available = [lid for lid in LAYER_IDS if lid in layer_series]
        if len(available) < 2:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "breaks": {},
                "country_iso3": country_iso3,
                "reason": f"Need at least 2 layer series, got {len(available)}",
            }

        # Detect breaks in each layer
        layer_breaks = {}
        for lid in available:
            scores = np.array(layer_series[lid])
            if len(scores) < MIN_OBS:
                continue
            breaks = self._detect_breaks_cusum(scores, alpha)
            layer_breaks[lid] = breaks

        # Find joint breaks (synchronous across layers)
        joint_breaks = self._find_joint_breaks(layer_breaks, available, lookback)

        # Identify regimes
        all_series_len = min(len(layer_series[lid]) for lid in available)
        regimes = self._identify_regimes(
            joint_breaks, layer_series, available, all_series_len
        )

        # Bai-Perron-style F-test for structural stability
        stability_tests = self._structural_stability(layer_series, available)

        # Score: more simultaneous breaks = higher score
        n_joint = len(joint_breaks)
        recent_break = any(
            b["position"] > all_series_len * 0.8 for b in joint_breaks
        )
        score = min(n_joint * 15.0 + (20.0 if recent_break else 0.0), 100.0)

        await self._store_result(
            db, country_iso3, score, layer_breaks, joint_breaks, regimes
        )

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "layer_breaks": {
                lid: [self._break_to_dict(b) for b in breaks]
                for lid, breaks in layer_breaks.items()
            },
            "joint_breaks": [self._break_to_dict(b) for b in joint_breaks],
            "regimes": regimes,
            "stability_tests": stability_tests,
            "recent_break_detected": recent_break,
            "layers_analyzed": available,
            "country_iso3": country_iso3,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    async def _fetch_layer_series(
        self, db, country_iso3: str, lookback: int
    ) -> dict[str, list[float]]:
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
                series[lid] = [r["score"] for r in reversed(rows)]
        return series

    def _detect_breaks_cusum(
        self, scores: np.ndarray, alpha: float
    ) -> list[dict]:
        """CUSUM test for structural breaks.

        Computes recursive residuals from a simple mean model, then
        tracks cumulative sum. A break is detected where the CUSUM
        exceeds critical bounds.
        """
        n = len(scores)
        min_seg = max(int(n * MIN_SEGMENT_FRAC), 3)
        breaks = []

        # OLS CUSUM: recursive residuals from expanding mean
        mean_full = np.mean(scores)
        std_full = np.std(scores, ddof=1) if n > 1 else 1.0
        if std_full < 1e-10:
            return []

        # Standardized recursive residuals
        cusum = np.cumsum(scores - mean_full) / (std_full * np.sqrt(n))

        # Critical value (Brownian bridge): approximated
        # At significance alpha, critical bound ~ alpha-dependent constant
        # Using standard CUSUM bounds: +/- (a + 2*t/T) where a depends on alpha
        a = 1.143 if alpha >= 0.05 else 1.358  # 5% / 1% levels

        # Find crossings
        for t in range(min_seg, n - min_seg):
            bound = a + 2.0 * t / n
            if abs(cusum[t]) > bound:
                # Potential break point
                # Verify with Chow-type test
                f_stat, p_val = self._chow_test(scores, t)
                if p_val < alpha:
                    breaks.append({
                        "position": t,
                        "cusum_value": float(cusum[t]),
                        "f_statistic": round(float(f_stat), 4),
                        "p_value": round(float(p_val), 4),
                        "mean_before": round(float(np.mean(scores[:t])), 2),
                        "mean_after": round(float(np.mean(scores[t:])), 2),
                        "shift": round(
                            float(np.mean(scores[t:]) - np.mean(scores[:t])), 2
                        ),
                    })

        # Deduplicate: keep strongest break in each neighborhood
        return self._deduplicate_breaks(breaks, min_seg)

    def _chow_test(
        self, scores: np.ndarray, break_point: int
    ) -> tuple[float, float]:
        """Chow test for structural break at a given point."""
        n = len(scores)
        y1 = scores[:break_point]
        y2 = scores[break_point:]
        n1, n2 = len(y1), len(y2)

        if n1 < 2 or n2 < 2:
            return 0.0, 1.0

        # RSS from pooled model (single mean)
        rss_pooled = float(np.sum((scores - np.mean(scores)) ** 2))

        # RSS from split models
        rss1 = float(np.sum((y1 - np.mean(y1)) ** 2))
        rss2 = float(np.sum((y2 - np.mean(y2)) ** 2))
        rss_split = rss1 + rss2

        # F-statistic: k = 1 parameter (mean)
        k = 1
        if rss_split < 1e-10:
            return 0.0, 1.0

        f_stat = ((rss_pooled - rss_split) / k) / (rss_split / (n - 2 * k))

        if f_stat < 0:
            return 0.0, 1.0

        p_value = 1.0 - sp_stats.f.cdf(f_stat, k, n - 2 * k)
        return f_stat, p_value

    def _deduplicate_breaks(
        self, breaks: list[dict], min_distance: int
    ) -> list[dict]:
        """Keep only the most significant break in each neighborhood."""
        if not breaks:
            return []

        sorted_breaks = sorted(breaks, key=lambda b: b["p_value"])
        kept = []
        used_positions = set()

        for b in sorted_breaks:
            pos = b["position"]
            # Check if too close to an already-kept break
            too_close = any(
                abs(pos - used) < min_distance for used in used_positions
            )
            if not too_close:
                kept.append(b)
                used_positions.add(pos)

        return sorted(kept, key=lambda b: b["position"])

    def _find_joint_breaks(
        self, layer_breaks: dict[str, list[dict]],
        available: list[str],
        max_len: int,
    ) -> list[dict]:
        """Find time points where multiple layers break simultaneously.

        Two breaks are "simultaneous" if within 2 periods of each other.
        """
        proximity = 2
        threshold = max(2, int(len(available) * JOINT_BREAK_THRESHOLD))

        # Collect all break positions
        all_positions = []
        for lid, breaks in layer_breaks.items():
            for b in breaks:
                all_positions.append((b["position"], lid, b))

        if not all_positions:
            return []

        all_positions.sort(key=lambda x: x[0])

        # Cluster nearby breaks
        joint = []
        i = 0
        while i < len(all_positions):
            cluster_pos = all_positions[i][0]
            cluster_layers = set()
            cluster_members = []

            j = i
            while j < len(all_positions) and all_positions[j][0] <= cluster_pos + proximity:
                cluster_layers.add(all_positions[j][1])
                cluster_members.append(all_positions[j])
                j += 1

            if len(cluster_layers) >= threshold:
                avg_pos = int(np.mean([m[0] for m in cluster_members]))
                joint.append({
                    "position": avg_pos,
                    "layers_affected": sorted(cluster_layers),
                    "n_layers": len(cluster_layers),
                    "fraction": round(len(cluster_layers) / len(available), 2),
                    "members": [
                        {"layer": m[1], "position": m[0], "shift": m[2]["shift"]}
                        for m in cluster_members
                    ],
                })

            i = j

        return joint

    def _identify_regimes(
        self, joint_breaks: list[dict],
        layer_series: dict[str, list[float]],
        available: list[str],
        series_len: int,
    ) -> list[dict]:
        """Identify distinct regimes between joint break points."""
        break_positions = sorted(set(b["position"] for b in joint_breaks))

        # Create regime boundaries
        boundaries = [0] + break_positions + [series_len]
        regimes = []

        for i in range(len(boundaries) - 1):
            start = boundaries[i]
            end = boundaries[i + 1]
            if end - start < 2:
                continue

            # Compute average scores per layer in this regime
            layer_avgs = {}
            for lid in available:
                if lid not in layer_series:
                    continue
                series = layer_series[lid]
                segment = series[start:end]
                if segment:
                    layer_avgs[lid] = round(float(np.mean(segment)), 2)

            overall_avg = float(np.mean(list(layer_avgs.values()))) if layer_avgs else 50.0

            regimes.append({
                "regime_id": i + 1,
                "start": start,
                "end": end,
                "length": end - start,
                "layer_averages": layer_avgs,
                "overall_average": round(overall_avg, 2),
                "classification": self.classify_signal(overall_avg),
            })

        return regimes

    def _structural_stability(
        self, layer_series: dict[str, list[float]], available: list[str]
    ) -> dict:
        """Quandt-Andrews-style sup-F test for unknown break date."""
        results = {}
        for lid in available:
            if lid not in layer_series:
                continue
            scores = np.array(layer_series[lid])
            n = len(scores)
            if n < MIN_OBS:
                results[lid] = {"stable": True, "sup_f": 0.0, "break_date": None}
                continue

            trim = max(int(n * MIN_SEGMENT_FRAC), 3)
            max_f = 0.0
            best_pos = None

            for t in range(trim, n - trim):
                f_stat, _ = self._chow_test(scores, t)
                if f_stat > max_f:
                    max_f = f_stat
                    best_pos = t

            # Critical values approximated from Andrews (1993) tables
            # For 1 parameter, 15% trimming, 5% significance: ~8.85
            critical = 8.85
            results[lid] = {
                "stable": max_f < critical,
                "sup_f": round(float(max_f), 4),
                "break_date_index": best_pos,
                "critical_value": critical,
            }

        return results

    @staticmethod
    def _break_to_dict(b: dict) -> dict:
        """Ensure break dict is JSON-serializable."""
        return {k: (round(v, 4) if isinstance(v, float) else v) for k, v in b.items()}

    async def _store_result(
        self, db, country_iso3: str, score: float,
        layer_breaks: dict, joint_breaks: list, regimes: list,
    ):
        await db.execute(
            """
            INSERT INTO analysis_results (analysis_type, country_iso3, layer, parameters, result, score, signal)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "cross_layer_break",
                country_iso3,
                "l6",
                json.dumps({"method": "cusum_chow_joint"}),
                json.dumps({
                    "n_joint_breaks": len(joint_breaks),
                    "n_regimes": len(regimes),
                    "layer_break_counts": {
                        lid: len(breaks) for lid, breaks in layer_breaks.items()
                    },
                }),
                round(score, 2),
                self.classify_signal(score),
            ),
        )
