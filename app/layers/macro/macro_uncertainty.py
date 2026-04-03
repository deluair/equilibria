"""Macro Uncertainty module.

Methodology
-----------
Realized macroeconomic uncertainty computed as a composite of volatility across
three key macro variables: GDP growth, inflation, and investment/GDP.

**Concept** (Jurado, Ludvigson & Ng 2015; Bloom 2009):
Macro uncertainty rises when the variance of fundamental macro series increases.
High uncertainty leads to:
    - Investment delays (real options channel, Dixit & Pindyck 1994)
    - Precautionary saving by households
    - Tighter credit conditions
    - Lower hiring by firms

**Computation**:
    For each series x in {GDP growth, inflation, investment/GDP}:
        - sigma_x = rolling std deviation over a 10-year window (or full sample)
        - Normalize to [0, 1] using max observed std over sample

    Composite uncertainty index:
        U = (sigma_gdp_norm + sigma_inf_norm + sigma_inv_norm) / 3 * 100

    Score = U (already 0-100).

**Additional diagnostics**:
    - Cross-series correlation: uncertainty tends to be synchronized across
      variables during crisis periods
    - Trend in volatility: rising volatility is itself a stress signal
    - Contribution of each variable to composite score

Sources: WDI (NY.GDP.MKTP.KD.ZG, FP.CPI.TOTL.ZG, NE.GDI.TOTL.ZS)
"""

import numpy as np

from app.layers.base import LayerBase


def _rolling_std(arr: np.ndarray, window: int) -> np.ndarray:
    """Compute rolling standard deviation, returning same-length array (NaN-padded at start)."""
    n = len(arr)
    out = np.full(n, np.nan)
    for i in range(window - 1, n):
        out[i] = float(np.std(arr[i - window + 1 : i + 1], ddof=1))
    return out


class MacroUncertainty(LayerBase):
    layer_id = "l2"
    name = "Macro Uncertainty"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        rolling_window = kwargs.get("rolling_window", 10)

        series_codes = {
            "gdp_growth": f"GDP_GROWTH_{country}",
            "inflation": f"INFLATION_{country}",
            "investment_gdp": f"INVEST_GDP_{country}",
        }

        raw: dict[str, dict[str, float]] = {}
        for label, code in series_codes.items():
            rows = await db.execute_fetchall(
                "SELECT date, value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE code = ?) ORDER BY date",
                (code,),
            )
            if rows:
                raw[label] = {r[0]: float(r[1]) for r in rows}

        if not raw:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"no macro data available for {country}",
            }

        # Align to common dates across available series
        available = list(raw.keys())
        common_dates = sorted(set.intersection(*[set(raw[k].keys()) for k in available]))

        if len(common_dates) < rolling_window + 2:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"insufficient aligned data for {country} (need at least {rolling_window + 2} obs)",
            }

        results = {
            "country": country,
            "n_obs": len(common_dates),
            "period": f"{common_dates[0]} to {common_dates[-1]}",
            "series_available": available,
            "rolling_window": rolling_window,
        }

        # Build arrays
        arrays: dict[str, np.ndarray] = {
            k: np.array([raw[k][d] for d in common_dates]) for k in available
        }

        # Full-sample standard deviations
        full_std: dict[str, float] = {k: float(np.std(v, ddof=1)) for k, v in arrays.items()}
        results["full_sample_std"] = {k: round(v, 4) for k, v in full_std.items()}

        # Rolling standard deviations (current = last observation)
        rolling_stds: dict[str, np.ndarray] = {
            k: _rolling_std(v, rolling_window) for k, v in arrays.items()
        }

        current_rolling_std: dict[str, float] = {}
        for k, rs in rolling_stds.items():
            valid = rs[~np.isnan(rs)]
            current_rolling_std[k] = float(valid[-1]) if len(valid) > 0 else float(full_std[k])

        results["current_rolling_std"] = {k: round(v, 4) for k, v in current_rolling_std.items()}

        # Normalize each rolling std by its max over the full rolling series
        # (gives a 0-1 scale where 1 = highest-ever recorded volatility)
        normalized: dict[str, float] = {}
        for k, rs in rolling_stds.items():
            valid = rs[~np.isnan(rs)]
            if len(valid) == 0:
                normalized[k] = 0.5
                continue
            max_val = float(np.max(valid))
            if max_val < 1e-10:
                normalized[k] = 0.0
            else:
                normalized[k] = float(np.clip(current_rolling_std[k] / max_val, 0.0, 1.0))

        results["normalized_volatility"] = {k: round(v, 4) for k, v in normalized.items()}

        # Composite uncertainty index (simple average of normalized vols)
        composite = float(np.mean(list(normalized.values()))) * 100.0

        # Volatility trend: compare last-quarter rolling std to 2-year-ago
        trend_signals: dict[str, str] = {}
        for k, rs in rolling_stds.items():
            valid = rs[~np.isnan(rs)]
            if len(valid) >= rolling_window + 8:
                old_std = float(valid[-rolling_window - 8])
                curr_std = float(valid[-1])
                trend_signals[k] = "rising" if curr_std > old_std * 1.1 else (
                    "falling" if curr_std < old_std * 0.9 else "stable"
                )
            else:
                trend_signals[k] = "indeterminate"

        results["volatility_trends"] = trend_signals

        # Penalty: if majority of series are showing rising volatility
        rising_count = sum(1 for t in trend_signals.values() if t == "rising")
        trend_penalty = min(rising_count * 5.0, 15.0)

        # Cross-series correlation (if 2+ series available)
        if len(available) >= 2:
            min_len = min(len(v) for v in arrays.values())
            arr_stack = np.column_stack([arrays[k][-min_len:] for k in available])
            corr_matrix = np.corrcoef(arr_stack.T)
            # Mean off-diagonal correlation (synchronization signal)
            n_s = len(available)
            off_diag = [
                corr_matrix[i, j]
                for i in range(n_s) for j in range(n_s) if i != j
            ]
            mean_cross_corr = float(np.mean(off_diag)) if off_diag else 0.0
            results["cross_series_correlation"] = {
                "mean": round(mean_cross_corr, 4),
                "high_synchronization": mean_cross_corr > 0.5,
            }
            # High synchronization (crisis co-movement) adds to stress
            sync_penalty = 10.0 if mean_cross_corr > 0.5 else 0.0
        else:
            sync_penalty = 0.0

        results["contributions"] = {k: round(normalized[k] * 100.0 / max(len(normalized), 1), 2)
                                    for k in normalized}
        results["composite_uncertainty_index"] = round(composite, 2)

        score = float(np.clip(composite + trend_penalty + sync_penalty, 0.0, 100.0))

        return {"score": round(score, 1), "results": results}
