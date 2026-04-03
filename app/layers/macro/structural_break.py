"""Structural Break Detection - Bai-Perron, CUSUM, Chow tests for regime changes."""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class StructuralBreak(LayerBase):
    layer_id = "l2"
    name = "Structural Break Detection"
    weight = 0.05

    # Series to test for structural breaks
    DEFAULT_SERIES = [
        "GDP",        # Real GDP
        "CPIAUCSL",   # CPI
        "FEDFUNDS",   # Fed Funds rate
        "UNRATE",     # Unemployment rate
        "INDPRO",     # Industrial Production
    ]

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        lookback = kwargs.get("lookback_years", 30)
        series_ids = kwargs.get("series_ids", self.DEFAULT_SERIES)
        max_breaks = kwargs.get("max_breaks", 5)
        trim_pct = kwargs.get("trim_pct", 0.15)
        significance = kwargs.get("significance", 0.05)

        rows = await db.execute_fetchall(
            """
            SELECT series_id, date, value FROM data_points
            WHERE series_id IN ({})
              AND country_code = ?
              AND date >= date('now', ?)
            ORDER BY series_id, date
            """.format(",".join("?" for _ in series_ids)),
            (*series_ids, country, f"-{lookback} years"),
        )

        series_map: dict[str, list[tuple[str, float]]] = {}
        for r in rows:
            series_map.setdefault(r["series_id"], []).append(
                (r["date"], float(r["value"]))
            )

        results = {}
        all_breaks = []

        for sid in series_ids:
            if sid not in series_map or len(series_map[sid]) < 20:
                continue

            dates = [d for d, _ in series_map[sid]]
            values = np.array([v for _, v in series_map[sid]])

            series_result = {}

            # Bai-Perron sequential structural break test
            bp_breaks = self._bai_perron(values, max_breaks, trim_pct, significance)
            series_result["bai_perron"] = {
                "n_breaks": len(bp_breaks),
                "break_indices": bp_breaks,
                "break_dates": [dates[i] for i in bp_breaks if i < len(dates)],
                "f_statistics": self._bai_perron_fstats(values, bp_breaks),
            }

            # CUSUM test
            cusum = self._cusum_test(values)
            series_result["cusum"] = cusum

            # Chow test at each Bai-Perron break
            chow_results = []
            for bp in bp_breaks:
                if 5 < bp < len(values) - 5:
                    chow = self._chow_test(values, bp)
                    chow["break_date"] = dates[bp]
                    chow_results.append(chow)
            series_result["chow_tests"] = chow_results

            # Regime characterization
            regimes = self._characterize_regimes(values, dates, bp_breaks)
            series_result["regimes"] = regimes

            results[sid] = series_result
            all_breaks.extend(
                [(dates[i], sid) for i in bp_breaks if i < len(dates)]
            )

        # Cluster breaks across series (breaks within 6 months = same event)
        clustered = self._cluster_breaks(all_breaks)
        results["clustered_breaks"] = clustered

        # Count recent breaks (last 3 years)
        recent_breaks = sum(
            1 for b in all_breaks
            if b[0] >= (sorted(all_breaks, key=lambda x: x[0])[-1][0][:4] if all_breaks else "9999")
        )

        # Score: more breaks = more regime uncertainty = higher stress
        n_total = len(all_breaks)
        n_clustered = len(clustered)
        score = float(np.clip(
            20.0 + n_clustered * 8.0 + recent_breaks * 10.0,
            0, 100
        ))

        return {
            "score": score,
            "results": results,
        }

    def _bai_perron(self, y: np.ndarray, max_breaks: int,
                    trim: float, significance: float) -> list[int]:
        """Sequential Bai-Perron structural break detection.
        Tests for breaks in the mean of a series using sup-F statistics."""
        n = len(y)
        min_segment = max(int(n * trim), 5)
        breaks = []

        # Sequential testing: find one break at a time
        remaining_y = y.copy()
        offset = 0

        for _ in range(max_breaks):
            best_idx, best_f = self._find_single_break(remaining_y, min_segment)
            if best_idx is None:
                break

            # Critical values for sup-F test (Andrews 1993, approximate)
            # For 5% significance with 1 break: ~8.85 for 1 regressor
            df1 = 1
            df2 = len(remaining_y) - 2
            critical = sp_stats.f.ppf(1 - significance, df1, df2) * 1.5  # sup adjustment

            if best_f > critical:
                global_idx = offset + best_idx
                breaks.append(global_idx)

                # Split and test sub-segments (take the larger one)
                if best_idx >= len(remaining_y) - best_idx:
                    remaining_y = remaining_y[:best_idx]
                else:
                    offset += best_idx
                    remaining_y = remaining_y[best_idx:]
            else:
                break

        return sorted(breaks)

    @staticmethod
    def _find_single_break(y: np.ndarray, min_segment: int) -> tuple:
        """Find the single most likely break point using sup-F statistic."""
        n = len(y)
        if n < 2 * min_segment:
            return None, 0.0

        best_f = 0.0
        best_idx = None

        # Full sample RSS
        y_mean = np.mean(y)
        rss_full = np.sum((y - y_mean) ** 2)

        if rss_full < 1e-12:
            return None, 0.0

        for t in range(min_segment, n - min_segment):
            y1 = y[:t]
            y2 = y[t:]
            rss1 = np.sum((y1 - np.mean(y1)) ** 2)
            rss2 = np.sum((y2 - np.mean(y2)) ** 2)
            rss_break = rss1 + rss2

            # F-statistic for structural break
            f_stat = ((rss_full - rss_break) / 1) / (rss_break / (n - 2))

            if f_stat > best_f:
                best_f = f_stat
                best_idx = t

        return best_idx, best_f

    def _bai_perron_fstats(self, y: np.ndarray, breaks: list[int]) -> list[float]:
        """Compute F-statistics at each break point."""
        if not breaks:
            return []

        n = len(y)
        y_mean = np.mean(y)
        rss_full = np.sum((y - y_mean) ** 2)

        f_stats = []
        for bp in breaks:
            if bp <= 0 or bp >= n:
                f_stats.append(0.0)
                continue
            y1 = y[:bp]
            y2 = y[bp:]
            rss_break = np.sum((y1 - np.mean(y1)) ** 2) + np.sum((y2 - np.mean(y2)) ** 2)
            if rss_break < 1e-12:
                f_stats.append(float("inf"))
            else:
                f_stat = ((rss_full - rss_break) / 1) / (rss_break / (n - 2))
                f_stats.append(float(f_stat))

        return f_stats

    @staticmethod
    def _cusum_test(y: np.ndarray) -> dict:
        """CUSUM test for parameter instability.
        Based on recursive residuals from expanding-window OLS."""
        n = len(y)
        if n < 10:
            return {"stable": True, "max_cusum": 0.0}

        # Recursive residuals: e_t = (y_t - mean(y_1:t-1)) / sqrt(1 + 1/(t-1))
        rec_resid = []
        for t in range(2, n):
            y_bar = np.mean(y[:t])
            resid = (y[t] - y_bar) / np.sqrt(1 + 1.0 / t)
            rec_resid.append(resid)

        rec_resid = np.array(rec_resid)
        sigma = np.std(rec_resid, ddof=1)
        if sigma < 1e-12:
            return {"stable": True, "max_cusum": 0.0}

        # CUSUM: cumulative sum of standardized recursive residuals
        cusum = np.cumsum(rec_resid / sigma)

        # Significance bounds (5%): +/- 0.948 * sqrt(n) at endpoints
        # Linear boundaries from 0 to boundary at endpoint
        m = len(cusum)
        boundary = 0.948 * np.sqrt(m)
        t_scaled = np.arange(1, m + 1) / m
        upper = boundary * (t_scaled + 0.5)
        lower = -upper

        max_cusum = float(np.max(np.abs(cusum)))
        crossings = np.where((cusum > upper) | (cusum < lower))[0]

        return {
            "stable": len(crossings) == 0,
            "max_cusum": max_cusum,
            "boundary_5pct": float(boundary),
            "n_boundary_crossings": len(crossings),
            "first_crossing_idx": int(crossings[0]) if len(crossings) > 0 else None,
        }

    @staticmethod
    def _chow_test(y: np.ndarray, break_point: int) -> dict:
        """Chow test for structural break at a known point."""
        n = len(y)
        y1 = y[:break_point]
        y2 = y[break_point:]
        n1, n2 = len(y1), len(y2)

        if n1 < 3 or n2 < 3:
            return {"f_statistic": 0.0, "p_value": 1.0, "significant": False}

        # RSS under restriction (pooled) and unrestriction (separate)
        rss_r = np.sum((y - np.mean(y)) ** 2)
        rss_ur = np.sum((y1 - np.mean(y1)) ** 2) + np.sum((y2 - np.mean(y2)) ** 2)

        k = 1  # number of parameters per equation (just mean)
        df1 = k
        df2 = n - 2 * k

        if rss_ur < 1e-12:
            return {"f_statistic": float("inf"), "p_value": 0.0, "significant": True}

        f_stat = ((rss_r - rss_ur) / df1) / (rss_ur / df2)
        p_value = 1 - sp_stats.f.cdf(f_stat, df1, df2)

        return {
            "f_statistic": float(f_stat),
            "p_value": float(p_value),
            "significant": p_value < 0.05,
            "break_index": break_point,
        }

    @staticmethod
    def _characterize_regimes(y: np.ndarray, dates: list[str],
                              breaks: list[int]) -> list[dict]:
        """Characterize statistical properties of each regime between breaks."""
        boundaries = [0] + breaks + [len(y)]
        regimes = []

        for i in range(len(boundaries) - 1):
            start = boundaries[i]
            end = boundaries[i + 1]
            segment = y[start:end]

            if len(segment) < 2:
                continue

            regimes.append({
                "regime": i + 1,
                "start_date": dates[start] if start < len(dates) else None,
                "end_date": dates[end - 1] if end - 1 < len(dates) else None,
                "n_obs": len(segment),
                "mean": float(np.mean(segment)),
                "std": float(np.std(segment, ddof=1)),
                "trend": float(np.polyfit(range(len(segment)), segment, 1)[0]) if len(segment) >= 3 else 0.0,
            })

        return regimes

    @staticmethod
    def _cluster_breaks(breaks: list[tuple[str, str]], window_months: int = 6) -> list[dict]:
        """Cluster breaks from different series that occur within a time window."""
        if not breaks:
            return []

        # Sort by date
        sorted_breaks = sorted(breaks, key=lambda x: x[0])

        clusters = []
        current_cluster = [sorted_breaks[0]]

        for i in range(1, len(sorted_breaks)):
            date_curr = sorted_breaks[i][0]
            date_prev = current_cluster[-1][0]

            # Simple date proximity check (compare year-month strings)
            # Breaks within ~6 months considered same event
            y1, m1 = int(date_prev[:4]), int(date_prev[5:7]) if len(date_prev) >= 7 else 1
            y2, m2 = int(date_curr[:4]), int(date_curr[5:7]) if len(date_curr) >= 7 else 1
            months_apart = (y2 - y1) * 12 + (m2 - m1)

            if months_apart <= window_months:
                current_cluster.append(sorted_breaks[i])
            else:
                clusters.append({
                    "date_range": f"{current_cluster[0][0]} to {current_cluster[-1][0]}",
                    "series_affected": list(set(s for _, s in current_cluster)),
                    "n_series": len(set(s for _, s in current_cluster)),
                })
                current_cluster = [sorted_breaks[i]]

        # Last cluster
        if current_cluster:
            clusters.append({
                "date_range": f"{current_cluster[0][0]} to {current_cluster[-1][0]}",
                "series_affected": list(set(s for _, s in current_cluster)),
                "n_series": len(set(s for _, s in current_cluster)),
            })

        return clusters
