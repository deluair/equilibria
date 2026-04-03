"""Bounded Rationality module.

Models cognitive limitations in economic decision-making.

1. **Satisficing vs Maximizing** (Simon 1955):
   Tests whether agents optimize or satisfice (accept first
   satisfactory option). Measured via choice consistency and
   decision time proxies in economic data.

2. **Choice Overload** (Iyengar & Lepper 2000):
   Measures the paradox of choice: excessive options reduce
   decision quality and satisfaction. Estimated from the
   relationship between variety/complexity and outcomes.

3. **Attention Allocation Models**:
   Sparse attention (Gabaix 2014): agents attend to the most
   salient dimensions of a problem, ignoring smaller factors.
   Measured via response to large vs small shocks.

4. **Rational Inattention** (Sims 2003):
   Agents have finite information-processing capacity (Shannon
   entropy constraint). Optimal attention allocation implies
   coarser responses to complex signals. Estimated via mutual
   information between economic signals and behavioral responses.

Score reflects departure from full rationality: high choice
overload, low attention, satisficing patterns -> higher stress.

Sources: WDI, FRED (market data, policy response times, complexity measures)
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class BoundedRationality(LayerBase):
    layer_id = "l13"
    name = "Bounded Rationality"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        # Fetch multiple series for complexity/variety measure
        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id, ds.description
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.source IN ('fred', 'wdi')
            ORDER BY ds.series_id, dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 20:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        # Group by series
        series_data: dict[str, list[float]] = {}
        series_dates: dict[str, list[str]] = {}
        for r in rows:
            sid = r["series_id"]
            series_data.setdefault(sid, []).append(float(r["value"]))
            series_dates.setdefault(sid, []).append(r["date"])

        # Use the longest series as primary
        primary_sid = max(series_data, key=lambda s: len(series_data[s]))
        primary = np.array(series_data[primary_sid])
        n_series = len(series_data)
        dates = series_dates[primary_sid]

        results = {
            "country": country,
            "n_series": n_series,
            "n_obs_primary": len(primary),
            "period": f"{dates[0]} to {dates[-1]}",
        }

        # --- 1. Satisficing vs Maximizing ---
        satisficing = self._satisficing_test(primary)
        results["satisficing"] = satisficing

        # --- 2. Choice Overload ---
        overload = self._choice_overload(series_data)
        results["choice_overload"] = overload

        # --- 3. Attention Allocation (Gabaix 2014) ---
        attention = self._attention_allocation(primary)
        results["attention_allocation"] = attention

        # --- 4. Rational Inattention (Sims 2003) ---
        inattention = self._rational_inattention(series_data)
        results["rational_inattention"] = inattention

        # --- Score ---
        satisfice_penalty = 20 if satisficing["is_satisficing"] else 5
        overload_penalty = min(25, overload["overload_index"] * 25)
        attention_penalty = max(0, min(25, (1 - attention["attention_ratio"]) * 50))
        inattention_penalty = max(0, min(30, (1 - inattention["mutual_info_ratio"]) * 60))

        score = min(100, satisfice_penalty + overload_penalty + attention_penalty + inattention_penalty)

        return {"score": round(score, 1), **results}

    @staticmethod
    def _satisficing_test(values: np.ndarray) -> dict:
        """Test for satisficing behavior (Simon 1955).

        Satisficers accept the first option meeting an aspiration level
        rather than optimizing. In time series, this manifests as:
        - Sticky behavior around threshold levels (aspiration points)
        - Low variance conditional on being above threshold
        - Asymmetric adjustment speed (fast below aspiration, slow above)
        """
        n = len(values)
        if n < 10:
            return {"is_satisficing": False, "note": "insufficient data"}

        median = float(np.median(values))
        above = values[values >= median]
        below = values[values < median]

        # Test 1: variance asymmetry (satisficers have low variance above threshold)
        var_above = float(np.var(above, ddof=1)) if len(above) > 1 else 0
        var_below = float(np.var(below, ddof=1)) if len(below) > 1 else 0
        var_ratio = var_above / max(var_below, 1e-10)

        # Test 2: stickiness (autocorrelation above threshold)
        if len(above) >= 5:
            ac_above = float(np.corrcoef(above[:-1], above[1:])[0, 1])
        else:
            ac_above = 0

        # Test 3: adjustment asymmetry
        changes = np.diff(values)
        # Speed of adjustment when below vs above median
        below_idx = np.where(values[:-1] < median)[0]
        above_idx = np.where(values[:-1] >= median)[0]
        adj_below = float(np.mean(np.abs(changes[below_idx]))) if len(below_idx) > 0 else 0
        adj_above = float(np.mean(np.abs(changes[above_idx]))) if len(above_idx) > 0 else 0

        # Satisficing: low variance above, high autocorrelation above, fast adjustment below
        is_satisficing = var_ratio < 0.7 and ac_above > 0.5

        return {
            "is_satisficing": bool(is_satisficing),
            "aspiration_proxy": round(median, 4),
            "var_ratio_above_below": round(var_ratio, 4),
            "autocorr_above_threshold": round(ac_above, 4),
            "adj_speed_below": round(adj_below, 4),
            "adj_speed_above": round(adj_above, 4),
            "reference": "Simon 1955, satisficing as bounded optimality",
        }

    @staticmethod
    def _choice_overload(series_data: dict[str, list[float]]) -> dict:
        """Measure choice overload (Iyengar & Lepper 2000).

        More series/dimensions = more complexity. Test whether
        outcome volatility increases with the number of signals
        agents must process.
        """
        n_series = len(series_data)

        # Compute volatility for each series
        volatilities = []
        for sid, vals in series_data.items():
            arr = np.array(vals)
            if len(arr) > 1:
                cv = float(np.std(arr, ddof=1) / max(abs(np.mean(arr)), 1e-10))
                volatilities.append(cv)

        mean_vol = float(np.mean(volatilities)) if volatilities else 0

        # Cross-series correlation (higher = simpler effective choice space)
        if n_series >= 2:
            # Find common length
            min_len = min(len(v) for v in series_data.values())
            if min_len >= 5:
                aligned = np.array([np.array(v[:min_len]) for v in series_data.values()])
                corr_matrix = np.corrcoef(aligned)
                # Average off-diagonal correlation
                n = corr_matrix.shape[0]
                off_diag = corr_matrix[np.triu_indices(n, k=1)]
                mean_corr = float(np.mean(off_diag)) if len(off_diag) > 0 else 0
                effective_dimensions = n * (1 - abs(mean_corr))
            else:
                mean_corr = 0
                effective_dimensions = float(n_series)
        else:
            mean_corr = 0
            effective_dimensions = 1.0

        # Overload index: normalized complexity measure
        # Higher effective dimensions + higher volatility = more overload
        overload_index = min(1.0, (effective_dimensions / 10) * (1 + mean_vol))

        return {
            "n_signals": n_series,
            "effective_dimensions": round(effective_dimensions, 2),
            "mean_volatility": round(mean_vol, 4),
            "mean_cross_correlation": round(mean_corr, 4),
            "overload_index": round(overload_index, 4),
            "interpretation": "high overload: decision quality likely impaired"
            if overload_index > 0.6
            else "manageable complexity",
        }

    @staticmethod
    def _attention_allocation(values: np.ndarray) -> dict:
        """Sparse attention model (Gabaix 2014).

        Tests whether agents respond proportionally to shock magnitude
        or exhibit threshold attention (only reacting to large shocks).
        """
        n = len(values)
        if n < 10:
            return {"attention_ratio": 0.5, "note": "insufficient data"}

        changes = np.diff(values)
        abs_changes = np.abs(changes)
        median_change = float(np.median(abs_changes))

        # Classify shocks as large (above median) or small (below)
        large_idx = abs_changes >= median_change
        small_idx = ~large_idx

        # Response: next-period adjustment magnitude
        if len(changes) < 2:
            return {"attention_ratio": 0.5, "note": "insufficient changes"}

        responses = np.abs(np.diff(values[1:]))
        min_len = min(len(responses), len(abs_changes) - 1)

        if min_len < 4:
            return {"attention_ratio": 0.5, "note": "insufficient response data"}

        shocks = abs_changes[:min_len]
        resp = responses[:min_len]
        large_mask = shocks >= median_change
        small_mask = ~large_mask

        resp_large = float(np.mean(resp[large_mask])) if np.sum(large_mask) > 0 else 0
        resp_small = float(np.mean(resp[small_mask])) if np.sum(small_mask) > 0 else 0

        # Attention ratio: response to small / response to large
        # Full attention -> ratio ~ 1 (proportional response)
        # Sparse attention -> ratio << 1 (ignore small shocks)
        attention_ratio = resp_small / max(resp_large, 1e-10)
        attention_ratio = min(1.0, max(0.0, attention_ratio))

        # Regression: response = a + b * shock_size
        # Under full attention, R-squared should be high
        X = np.column_stack([np.ones(min_len), shocks])
        beta = np.linalg.lstsq(X, resp, rcond=None)[0]
        predicted = X @ beta
        ss_res = float(np.sum((resp - predicted) ** 2))
        ss_tot = float(np.sum((resp - np.mean(resp)) ** 2))
        r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0

        return {
            "attention_ratio": round(attention_ratio, 4),
            "response_to_large": round(resp_large, 4),
            "response_to_small": round(resp_small, 4),
            "shock_response_r2": round(r2, 4),
            "sparse_attention": attention_ratio < 0.5,
            "reference": "Gabaix 2014: sparsity of attention in economic decisions",
        }

    @staticmethod
    def _rational_inattention(series_data: dict[str, list[float]]) -> dict:
        """Rational inattention model (Sims 2003).

        Agents have finite Shannon capacity for processing information.
        Optimal behavior under capacity constraints implies coarser
        responses to complex signals.

        Measures mutual information between input signals (data series)
        and output (behavioral response proxy).
        """
        if len(series_data) < 2:
            return {"mutual_info_ratio": 0.5, "note": "need multiple series"}

        # Use first series as "behavior" (output), rest as "signals" (inputs)
        series_list = list(series_data.values())
        min_len = min(len(s) for s in series_list)
        if min_len < 10:
            return {"mutual_info_ratio": 0.5, "note": "insufficient data"}

        output = np.array(series_list[0][:min_len])
        inputs = [np.array(s[:min_len]) for s in series_list[1:]]

        # Compute entropy of output (discretized)
        n_bins = max(5, min(20, min_len // 5))
        output_hist, _ = np.histogram(output, bins=n_bins, density=True)
        output_hist = output_hist[output_hist > 0]
        h_output = float(-np.sum(output_hist * np.log2(output_hist + 1e-15)))

        # Compute mutual information between each input and output
        mutual_infos = []
        for inp in inputs:
            # Joint histogram
            joint_hist, _, _ = np.histogram2d(inp, output, bins=n_bins, density=True)
            joint_hist = joint_hist[joint_hist > 0]
            h_joint = float(-np.sum(joint_hist * np.log2(joint_hist + 1e-15)))

            inp_hist, _ = np.histogram(inp, bins=n_bins, density=True)
            inp_hist = inp_hist[inp_hist > 0]
            h_input = float(-np.sum(inp_hist * np.log2(inp_hist + 1e-15)))

            mi = max(0, h_input + h_output - h_joint)
            mutual_infos.append(mi)

        mean_mi = float(np.mean(mutual_infos)) if mutual_infos else 0
        max_possible_mi = h_output if h_output > 0 else 1

        # MI ratio: how much of available information is actually used
        mi_ratio = min(1.0, mean_mi / max(max_possible_mi, 1e-10))

        # Channel capacity proxy: total MI across all inputs
        total_mi = float(np.sum(mutual_infos))

        return {
            "mutual_info_ratio": round(mi_ratio, 4),
            "mean_mutual_info_bits": round(mean_mi, 4),
            "total_channel_capacity_bits": round(total_mi, 4),
            "output_entropy_bits": round(h_output, 4),
            "n_input_signals": len(inputs),
            "capacity_constrained": mi_ratio < 0.3,
            "reference": "Sims 2003: implications of rational inattention",
        }
