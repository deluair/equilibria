"""Regression Kink Design (Card, Lee, Pei & Weber 2015).

The RKD exploits a known kink (change in slope) in the relationship between
an assignment variable and a policy variable at a threshold. Unlike RDD which
exploits a jump in levels, RKD exploits a change in the first derivative.

The treatment effect is the ratio of the kink in the outcome to the kink in
the policy variable (a fuzzy kink Wald estimator):

    tau_RKD = lim_{z->c} [dE[Y|Z=z]/dz from right - dE[Y|Z=z]/dz from left]
              / [dB[Z=z]/dz from right - dB[Z=z]/dz from left]

Applications: unemployment insurance benefit schedules (earnings-replacement
kinks), tax credit phase-ins/phase-outs, student aid formulas.

Key implementation:
    1. Local polynomial regression above/below kink point
    2. Estimate slope from each side, take difference
    3. Bandwidth selection via IK/CCT procedures
    4. McCrary (2008) density test for manipulation
    5. Placebo kink tests at non-kink points

References:
    Card, D., Lee, D., Pei, Z. & Weber, A. (2015). Inference on Causal
        Effects in a Generalized Regression Kink Design. Econometrica
        83(6): 2453-2483.
    Nielsen, H., Sorensen, T. & Taber, C. (2010). Estimating the Effect
        of Student Aid on College Enrollment: Evidence from a Government
        Grant Policy Reform. AEJ: Economic Policy 2(2): 185-215.
    McCrary, J. (2008). Manipulation of the Running Variable in the
        Regression Discontinuity Design. JoE 142(2): 698-714.

Score: large kink in outcome relative to first-stage kink -> high score
(strong policy effect). Weak or no kink -> STABLE.
"""

import json

import numpy as np

from app.layers.base import LayerBase


class RegressionKinkDesign(LayerBase):
    layer_id = "l18"
    name = "Regression Kink Design"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        kink_point = kwargs.get("kink_point")
        bandwidth = kwargs.get("bandwidth")

        rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'regression_kink'
            ORDER BY dp.value
            """,
            (country,),
        )

        if not rows or len(rows) < 30:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient RKD data"}

        # Parse running variable (z), outcome (y), and policy variable (b)
        z_vals, y_vals, b_vals = [], [], []
        for row in rows:
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            z = meta.get("running_var")
            y = meta.get("outcome")
            b = meta.get("policy_var")
            if z is None or y is None:
                continue
            z_vals.append(float(z))
            y_vals.append(float(y))
            b_vals.append(float(b)) if b is not None else b_vals.append(None)

        z = np.array(z_vals)
        y = np.array(y_vals)
        has_policy = all(v is not None for v in b_vals)
        b = np.array(b_vals) if has_policy else None
        n = len(z)

        if n < 30:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient valid obs"}

        # Detect kink point from metadata
        if kink_point is None:
            for row in rows:
                meta = json.loads(row["metadata"]) if row.get("metadata") else {}
                if meta.get("kink_point") is not None:
                    kink_point = float(meta["kink_point"])
                    break
        if kink_point is None:
            kink_point = float(np.median(z))

        # Bandwidth selection (IK-style rule of thumb)
        if bandwidth is None:
            bandwidth = self._ik_bandwidth(z, y, kink_point)

        # Select observations within bandwidth
        in_bw = np.abs(z - kink_point) <= bandwidth
        z_bw = z[in_bw]
        y_bw = y[in_bw]
        n_eff = int(np.sum(in_bw))

        if n_eff < 10:
            return {"score": None, "signal": "UNAVAILABLE", "error": "too few obs in bandwidth"}

        left = z_bw < kink_point
        right = z_bw >= kink_point

        if np.sum(left) < 3 or np.sum(right) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient obs on both sides"}

        # Local linear regression on each side
        slope_left_y, se_left_y = self._local_slope(z_bw[left] - kink_point, y_bw[left])
        slope_right_y, se_right_y = self._local_slope(z_bw[right] - kink_point, y_bw[right])

        # Kink in outcome
        kink_y = slope_right_y - slope_left_y
        se_kink_y = np.sqrt(se_left_y ** 2 + se_right_y ** 2)
        t_stat_y = kink_y / se_kink_y if se_kink_y > 1e-10 else 0.0

        # Fuzzy RKD: ratio of outcome kink to policy kink
        rkd_estimate = None
        rkd_se = None
        if b is not None:
            b_bw = b[in_bw]
            slope_left_b, se_left_b = self._local_slope(z_bw[left] - kink_point, b_bw[left])
            slope_right_b, se_right_b = self._local_slope(z_bw[right] - kink_point, b_bw[right])
            kink_b = slope_right_b - slope_left_b
            if abs(kink_b) > 1e-10:
                rkd_estimate = kink_y / kink_b
                # Delta method SE
                rkd_se = abs(rkd_estimate) * np.sqrt(
                    (se_kink_y / kink_y) ** 2 + ((se_left_b ** 2 + se_right_b ** 2) / kink_b ** 2)
                ) if abs(kink_y) > 1e-10 else None

        # McCrary density test for manipulation
        mccrary = self._mccrary_test(z, kink_point, bandwidth)

        # Placebo kink tests at 25th and 75th percentile
        placebo_results = []
        for pctl in [25, 75]:
            placebo_c = float(np.percentile(z, pctl))
            if abs(placebo_c - kink_point) < bandwidth * 0.5:
                continue
            in_plac = np.abs(z - placebo_c) <= bandwidth
            z_plac = z[in_plac]
            y_plac = y[in_plac]
            left_p = z_plac < placebo_c
            right_p = z_plac >= placebo_c
            if np.sum(left_p) < 3 or np.sum(right_p) < 3:
                continue
            sl, _ = self._local_slope(z_plac[left_p] - placebo_c, y_plac[left_p])
            sr, _ = self._local_slope(z_plac[right_p] - placebo_c, y_plac[right_p])
            placebo_results.append({
                "percentile": pctl,
                "kink_estimate": round(sr - sl, 4),
            })

        # Score: large, significant kink -> high score
        abs_t = abs(t_stat_y)
        if abs_t > 3.0:
            score = 60.0 + min(abs_t - 3.0, 4.0) * 10.0
        elif abs_t > 1.96:
            score = 30.0 + (abs_t - 1.96) * 28.8
        else:
            score = abs_t * 15.3
        score = max(0.0, min(100.0, score))

        return {
            "score": round(score, 2),
            "country": country,
            "kink_point": kink_point,
            "bandwidth": round(bandwidth, 4),
            "n_total": n,
            "n_effective": n_eff,
            "outcome_kink": {
                "estimate": round(kink_y, 4),
                "se": round(se_kink_y, 4),
                "t_stat": round(t_stat_y, 4),
                "slope_left": round(slope_left_y, 4),
                "slope_right": round(slope_right_y, 4),
            },
            "rkd_estimate": round(rkd_estimate, 4) if rkd_estimate is not None else None,
            "rkd_se": round(rkd_se, 4) if rkd_se is not None else None,
            "mccrary_test": mccrary,
            "placebo_kinks": placebo_results,
        }

    @staticmethod
    def _local_slope(x: np.ndarray, y: np.ndarray) -> tuple:
        """Local linear regression: y = a + b*x. Returns (slope, se_slope)."""
        n = len(x)
        if n < 2:
            return 0.0, float("inf")
        X = np.column_stack([np.ones(n), x])
        beta = np.linalg.lstsq(X, y, rcond=None)[0]
        resid = y - X @ beta
        sigma2 = float(np.sum(resid ** 2)) / max(n - 2, 1)
        try:
            var_beta = sigma2 * np.linalg.inv(X.T @ X)
            se_slope = float(np.sqrt(max(var_beta[1, 1], 0.0)))
        except np.linalg.LinAlgError:
            se_slope = float("inf")
        return float(beta[1]), se_slope

    @staticmethod
    def _ik_bandwidth(z: np.ndarray, y: np.ndarray, c: float) -> float:
        """Imbens-Kalyanaraman (2012) bandwidth selector (simplified)."""
        h_pilot = 1.06 * float(np.std(z)) * len(z) ** (-0.2)
        # Refine: use curvature of conditional mean
        in_pilot = np.abs(z - c) <= h_pilot
        if np.sum(in_pilot) < 10:
            return h_pilot
        z_p = z[in_pilot] - c
        y_p = y[in_pilot]
        X_p = np.column_stack([np.ones(len(z_p)), z_p, z_p ** 2])
        try:
            beta = np.linalg.lstsq(X_p, y_p, rcond=None)[0]
            curv = abs(beta[2])
        except Exception:
            curv = 1.0
        n = len(z)
        h_opt = (float(np.std(y)) / max(curv, 1e-6)) ** 0.2 * n ** (-0.2)
        return max(h_opt, h_pilot * 0.5)

    @staticmethod
    def _mccrary_test(z: np.ndarray, c: float, bw: float) -> dict:
        """McCrary (2008) density test for manipulation at kink point."""
        in_bw = np.abs(z - c) <= bw
        z_bw = z[in_bw]
        n_left = int(np.sum(z_bw < c))
        n_right = int(np.sum(z_bw >= c))
        n_tot = n_left + n_right
        if n_tot == 0:
            return {"log_diff": None, "p_value": None}
        # Log difference in density heights at cutoff
        f_left = n_left / (n_tot * bw) if bw > 0 else 0
        f_right = n_right / (n_tot * bw) if bw > 0 else 0
        if f_left > 0 and f_right > 0:
            log_diff = float(np.log(f_right) - np.log(f_left))
            # SE from binomial approximation
            se = float(np.sqrt(1.0 / max(n_left, 1) + 1.0 / max(n_right, 1)))
            t = log_diff / se if se > 0 else 0.0
            from scipy.stats import norm
            p_val = float(2.0 * (1.0 - norm.cdf(abs(t))))
        else:
            log_diff = None
            p_val = None
        return {
            "log_diff": round(log_diff, 4) if log_diff is not None else None,
            "p_value": round(p_val, 4) if p_val is not None else None,
            "n_left": n_left,
            "n_right": n_right,
        }
