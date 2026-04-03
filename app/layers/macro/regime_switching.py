"""Regime Switching - Hamilton (1989) Markov-switching model.

Methodology
-----------
Two-state Markov-switching model for GDP growth:

    y_t = mu_{s_t} + phi * y_{t-1} + sigma_{s_t} * e_t,  e_t ~ N(0,1)

where s_t in {0, 1} is the latent state (0=expansion, 1=recession).

State transitions follow a first-order Markov chain:
    P(s_t = j | s_{t-1} = i) = p_{ij}

Transition matrix:
    P = [[p00, p01],
         [p10, p11]]

where p00 = P(stay in expansion), p11 = P(stay in recession).

**Expected duration** of regime j: 1 / (1 - p_jj)

Estimation via Hamilton filter (forward recursion) with EM algorithm
for parameter estimation.

**Smoothed probabilities** via Kim (1994) smoother give the probability
of being in each state at each time point using the full sample.

References:
- Hamilton (1989), "A New Approach to the Economic Analysis of
  Nonstationary Time Series and the Business Cycle," Econometrica
- Kim (1994), "Dynamic Linear Models with Markov-Switching," J. Econometrics
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class RegimeSwitching(LayerBase):
    layer_id = "l2"
    name = "Regime Switching"
    weight = 0.05

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        lookback = kwargs.get("lookback_years", 40)
        n_restarts = kwargs.get("n_restarts", 5)

        rows = await db.execute_fetchall(
            """
            SELECT date, value FROM data_points
            WHERE series_id = (SELECT id FROM data_series WHERE code = ?)
              AND date >= date('now', ?)
            ORDER BY date
            """,
            (f"REAL_GROWTH_{country}", f"-{lookback} years"),
        )

        if not rows or len(rows) < 20:
            return {"score": 50.0, "results": {"error": "insufficient data"}}

        dates = [r[0] for r in rows]
        y = np.array([float(r[1]) for r in rows])
        T = len(y)

        results = {
            "country": country,
            "n_obs": T,
            "period": f"{dates[0]} to {dates[-1]}",
        }

        # Estimate with multiple restarts to avoid local optima
        best_ll = -np.inf
        best_params = None

        rng = np.random.default_rng(42)

        for restart in range(n_restarts):
            try:
                params, ll = self._estimate_em(y, rng, max_iter=200)
                if ll > best_ll:
                    best_ll = ll
                    best_params = params
            except Exception:
                continue

        if best_params is None:
            return {"score": 50.0, "results": results, "note": "estimation failed"}

        mu0, mu1, sigma0, sigma1, p00, p11, phi = best_params

        # Ensure state 0 = expansion (higher mean growth)
        if mu0 < mu1:
            mu0, mu1 = mu1, mu0
            sigma0, sigma1 = sigma1, sigma0
            p00, p11 = p11, p00

        results["parameters"] = {
            "mu_expansion": round(float(mu0), 4),
            "mu_recession": round(float(mu1), 4),
            "sigma_expansion": round(float(sigma0), 4),
            "sigma_recession": round(float(sigma1), 4),
            "p_stay_expansion": round(float(p00), 4),
            "p_stay_recession": round(float(p11), 4),
            "ar1_coefficient": round(float(phi), 4),
            "log_likelihood": round(float(best_ll), 2),
        }

        # Expected durations
        dur_expansion = 1.0 / (1.0 - p00) if p00 < 1.0 else float("inf")
        dur_recession = 1.0 / (1.0 - p11) if p11 < 1.0 else float("inf")

        results["regime_durations"] = {
            "expected_expansion_quarters": round(dur_expansion, 1),
            "expected_recession_quarters": round(dur_recession, 1),
            "expansion_to_recession_prob": round(1.0 - p00, 4),
            "recession_to_expansion_prob": round(1.0 - p11, 4),
        }

        # Ergodic (unconditional) probabilities
        pi_expansion = (1.0 - p11) / (2.0 - p00 - p11) if (2.0 - p00 - p11) > 0 else 0.5
        results["ergodic_probabilities"] = {
            "expansion": round(pi_expansion, 4),
            "recession": round(1.0 - pi_expansion, 4),
        }

        # Hamilton filter: filtered probabilities
        filtered = self._hamilton_filter(
            y, mu0, mu1, sigma0, sigma1, p00, p11, phi
        )

        # Kim smoother: smoothed probabilities
        smoothed = self._kim_smoother(filtered, p00, p11)

        # Current state assessment
        prob_recession_now = float(smoothed[-1])
        results["current_state"] = {
            "recession_probability": round(prob_recession_now, 4),
            "most_likely_state": "recession" if prob_recession_now > 0.5 else "expansion",
            "confidence": round(max(prob_recession_now, 1 - prob_recession_now), 4),
        }

        # Regime chronology
        regime_changes = self._identify_regimes(smoothed, dates)
        results["regime_chronology"] = regime_changes

        # Smoothed probabilities (subsample for output)
        step = max(1, T // 100)
        results["smoothed_probabilities"] = {
            "dates": [dates[i] for i in range(0, T, step)],
            "recession_prob": [round(float(smoothed[i]), 4) for i in range(0, T, step)],
        }

        # Model comparison: AIC/BIC vs single-regime AR(1)
        n_params_ms = 7  # mu0, mu1, sigma0, sigma1, p00, p11, phi
        aic_ms = -2 * best_ll + 2 * n_params_ms
        bic_ms = -2 * best_ll + np.log(T) * n_params_ms

        # Single-regime AR(1) for comparison
        X_ar = np.column_stack([np.ones(T - 1), y[:-1]])
        beta_ar = np.linalg.lstsq(X_ar, y[1:], rcond=None)[0]
        resid_ar = y[1:] - X_ar @ beta_ar
        sigma_ar = float(np.std(resid_ar, ddof=2))
        ll_ar = float(np.sum(sp_stats.norm.logpdf(resid_ar, 0, sigma_ar)))
        aic_ar = -2 * ll_ar + 2 * 3
        bic_ar = -2 * ll_ar + np.log(T - 1) * 3

        results["model_comparison"] = {
            "markov_switching": {
                "aic": round(aic_ms, 2),
                "bic": round(bic_ms, 2),
                "log_likelihood": round(float(best_ll), 2),
            },
            "single_regime_ar1": {
                "aic": round(aic_ar, 2),
                "bic": round(bic_ar, 2),
                "log_likelihood": round(ll_ar, 2),
            },
            "ms_preferred_aic": aic_ms < aic_ar,
            "ms_preferred_bic": bic_ms < bic_ar,
        }

        # Score: high recession probability = stress
        if prob_recession_now > 0.7:
            state_score = 70
        elif prob_recession_now > 0.5:
            state_score = 55
        elif prob_recession_now > 0.3:
            state_score = 35
        else:
            state_score = 15

        # Adjustment for transition probability (high risk of switching)
        switch_risk = 1.0 - p00  # probability of leaving expansion
        risk_adj = min(switch_risk * 30, 15)

        score = min(state_score + risk_adj, 100)

        return {"score": round(score, 1), "results": results}

    @staticmethod
    def _estimate_em(y: np.ndarray, rng: np.random.Generator,
                     max_iter: int = 200) -> tuple:
        """Estimate MS-AR(1) via EM algorithm."""
        T = len(y)

        # Initialize with random perturbations around reasonable values
        mu0 = float(rng.normal(2.0, 1.0))  # expansion mean
        mu1 = float(rng.normal(-1.0, 1.0))  # recession mean
        sigma0 = float(abs(rng.normal(1.5, 0.5)))
        sigma1 = float(abs(rng.normal(2.5, 0.5)))
        p00 = float(rng.uniform(0.8, 0.98))
        p11 = float(rng.uniform(0.6, 0.95))
        phi = float(rng.uniform(-0.3, 0.5))

        prev_ll = -np.inf

        for iteration in range(max_iter):
            # E-step: Hamilton filter
            # Filtered probabilities P(S_t=1 | y_1,...,y_t)
            xi_filt = np.zeros(T)  # P(recession)
            xi_pred = np.zeros(T)  # P(recession) predicted
            ll = 0.0

            # Initial from ergodic
            denom = (2.0 - p00 - p11)
            if abs(denom) < 1e-10:
                pi_0 = 0.5
            else:
                pi_0 = (1.0 - p00) / denom
            xi_pred[0] = pi_0

            for t in range(T):
                # Conditional densities
                resid0 = y[t] - mu0
                resid1 = y[t] - mu1
                if t > 0:
                    resid0 -= phi * y[t - 1]
                    resid1 -= phi * y[t - 1]

                f0 = sp_stats.norm.pdf(resid0, 0, max(sigma0, 1e-6))
                f1 = sp_stats.norm.pdf(resid1, 0, max(sigma1, 1e-6))

                # Joint density
                p_exp = (1.0 - xi_pred[t]) * f0
                p_rec = xi_pred[t] * f1
                f_t = p_exp + p_rec

                if f_t < 1e-300:
                    f_t = 1e-300

                ll += np.log(f_t)

                # Update
                xi_filt[t] = p_rec / f_t

                # Predict next
                if t < T - 1:
                    xi_pred[t + 1] = (1 - p00) * (1 - xi_filt[t]) + p11 * xi_filt[t]

            # Kim smoother for smoothed probabilities
            xi_smooth = np.zeros(T)
            xi_smooth[-1] = xi_filt[-1]

            for t in range(T - 2, -1, -1):
                pred_next = xi_pred[t + 1]
                if pred_next < 1e-10:
                    pred_next = 1e-10
                if (1 - pred_next) < 1e-10:
                    pred_next = 1 - 1e-10

                # Smoothed: P(S_t=1|Y_T)
                w1 = xi_filt[t] * p11 / pred_next
                w0 = (1 - xi_filt[t]) * (1 - p00) / pred_next

                xi_smooth[t] = (
                    xi_filt[t] * (p11 * xi_smooth[t + 1] / pred_next
                                  + (1 - p11) * (1 - xi_smooth[t + 1]) / (1 - pred_next))
                )
                xi_smooth[t] = np.clip(xi_smooth[t], 1e-10, 1 - 1e-10)

            # M-step: update parameters
            w1 = xi_smooth  # weight for recession
            w0 = 1.0 - xi_smooth  # weight for expansion

            # AR residuals
            if T > 1:
                y_adj0 = y.copy()
                y_adj1 = y.copy()
                y_adj0[1:] = y[1:] - phi * y[:-1]
                y_adj1[1:] = y[1:] - phi * y[:-1]
                y_adj0[0] = y[0]
                y_adj1[0] = y[0]
            else:
                y_adj0 = y
                y_adj1 = y

            # Means
            sum_w0 = max(np.sum(w0), 1e-10)
            sum_w1 = max(np.sum(w1), 1e-10)
            mu0 = float(np.sum(w0 * y_adj0) / sum_w0)
            mu1 = float(np.sum(w1 * y_adj1) / sum_w1)

            # Variances
            sigma0 = float(np.sqrt(np.sum(w0 * (y_adj0 - mu0) ** 2) / sum_w0))
            sigma1 = float(np.sqrt(np.sum(w1 * (y_adj1 - mu1) ** 2) / sum_w1))
            sigma0 = max(sigma0, 0.01)
            sigma1 = max(sigma1, 0.01)

            # Transition probabilities
            # P(S_t=j, S_{t-1}=i | Y_T)
            if T > 1:
                p00_num = float(np.sum(w0[1:] * w0[:-1]))
                p00_den = float(np.sum(w0[:-1]))
                p11_num = float(np.sum(w1[1:] * w1[:-1]))
                p11_den = float(np.sum(w1[:-1]))

                p00 = np.clip(p00_num / max(p00_den, 1e-10), 0.01, 0.99)
                p11 = np.clip(p11_num / max(p11_den, 1e-10), 0.01, 0.99)

            # AR coefficient
            if T > 1:
                num = float(np.sum(y[1:] * y[:-1]) - mu0 * np.sum(w0[1:] * y[:-1])
                            - mu1 * np.sum(w1[1:] * y[:-1]))
                den = float(np.sum(y[:-1] ** 2))
                if abs(den) > 1e-10:
                    phi = np.clip(num / den, -0.95, 0.95)

            # Check convergence
            if abs(ll - prev_ll) < 1e-6:
                break
            prev_ll = ll

        return (mu0, mu1, sigma0, sigma1, p00, p11, phi), ll

    @staticmethod
    def _hamilton_filter(y, mu0, mu1, sigma0, sigma1, p00, p11, phi):
        """Hamilton filter: return filtered P(recession) at each t."""
        T = len(y)
        xi = np.zeros(T)

        denom = (2.0 - p00 - p11)
        pi_0 = (1.0 - p00) / denom if abs(denom) > 1e-10 else 0.5
        xi_pred = pi_0

        for t in range(T):
            resid0 = y[t] - mu0 - (phi * y[t - 1] if t > 0 else 0.0)
            resid1 = y[t] - mu1 - (phi * y[t - 1] if t > 0 else 0.0)

            f0 = sp_stats.norm.pdf(resid0, 0, max(sigma0, 1e-6))
            f1 = sp_stats.norm.pdf(resid1, 0, max(sigma1, 1e-6))

            p_exp = (1.0 - xi_pred) * f0
            p_rec = xi_pred * f1
            f_t = max(p_exp + p_rec, 1e-300)

            xi[t] = p_rec / f_t
            xi_pred = (1 - p00) * (1 - xi[t]) + p11 * xi[t]

        return xi

    @staticmethod
    def _kim_smoother(filtered: np.ndarray, p00: float, p11: float) -> np.ndarray:
        """Kim (1994) smoother: smoothed probabilities using full sample."""
        T = len(filtered)
        smoothed = np.zeros(T)
        smoothed[-1] = filtered[-1]

        for t in range(T - 2, -1, -1):
            pred = (1 - p00) * (1 - filtered[t]) + p11 * filtered[t]
            pred = np.clip(pred, 1e-10, 1 - 1e-10)

            # P(S_t=1 | Y_T) via Bayes
            ratio = smoothed[t + 1] / pred
            ratio_comp = (1 - smoothed[t + 1]) / (1 - pred)

            smoothed[t] = filtered[t] * (p11 * ratio + (1 - p11) * ratio_comp)
            smoothed[t] = np.clip(smoothed[t], 0.0, 1.0)

        return smoothed

    @staticmethod
    def _identify_regimes(smoothed: np.ndarray, dates: list[str]) -> list[dict]:
        """Identify regime episodes from smoothed probabilities."""
        T = len(smoothed)
        regimes = []
        current_state = "recession" if smoothed[0] > 0.5 else "expansion"
        start_idx = 0

        for t in range(1, T):
            state = "recession" if smoothed[t] > 0.5 else "expansion"
            if state != current_state:
                regimes.append({
                    "state": current_state,
                    "start": dates[start_idx],
                    "end": dates[t - 1],
                    "duration_quarters": t - start_idx,
                    "avg_probability": round(float(np.mean(
                        smoothed[start_idx:t] if current_state == "recession"
                        else 1 - smoothed[start_idx:t]
                    )), 4),
                })
                current_state = state
                start_idx = t

        # Final regime
        regimes.append({
            "state": current_state,
            "start": dates[start_idx],
            "end": dates[-1],
            "duration_quarters": T - start_idx,
            "avg_probability": round(float(np.mean(
                smoothed[start_idx:] if current_state == "recession"
                else 1 - smoothed[start_idx:]
            )), 4),
        })

        return regimes
