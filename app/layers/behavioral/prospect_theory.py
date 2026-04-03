"""Prospect Theory estimation module.

Implements Kahneman-Tversky (1979) prospect theory framework:

1. **Value Function** estimation:
   v(x) = x^alpha              if x >= 0  (gains)
   v(x) = -lambda * (-x)^beta  if x < 0   (losses)

   Standard parameters: alpha = beta ~ 0.88, lambda ~ 2.25
   (Tversky & Kahneman 1992).

2. **Loss Aversion Coefficient** (lambda):
   Ratio of loss sensitivity to gain sensitivity. Lambda > 1
   indicates loss aversion. Estimated via median ratio of
   willingness-to-pay vs willingness-to-accept.

3. **Probability Weighting Function** (Prelec 1998):
   w(p) = exp(-(-ln(p))^gamma)
   gamma < 1 implies overweighting of small probabilities and
   underweighting of large probabilities (inverse S-shape).

4. **Reference Point Effects**:
   Tests whether economic decisions (trade, investment) exhibit
   reference-dependent utility: asymmetric responses to gains
   vs losses relative to status quo or historical norms.

Score reflects deviation from rational expectations benchmark:
large loss aversion or probability distortion -> higher stress score.

Sources: FRED, WDI (asset returns, trade balance changes, investment flows)
"""

from __future__ import annotations

import numpy as np
from scipy.optimize import minimize

from app.layers.base import LayerBase


def _prelec_weight(p: np.ndarray, gamma: float) -> np.ndarray:
    """Prelec (1998) probability weighting function.

    w(p) = exp(-(-ln(p))^gamma)
    """
    p = np.clip(p, 1e-10, 1 - 1e-10)
    return np.exp(-((-np.log(p)) ** gamma))


def _value_function(x: np.ndarray, alpha: float, beta: float, lam: float) -> np.ndarray:
    """Kahneman-Tversky value function.

    v(x) = x^alpha for gains (x >= 0)
    v(x) = -lambda * (-x)^beta for losses (x < 0)
    """
    v = np.zeros_like(x, dtype=float)
    gains = x >= 0
    losses = x < 0
    v[gains] = np.power(np.abs(x[gains]) + 1e-15, alpha)
    v[losses] = -lam * np.power(np.abs(x[losses]) + 1e-15, beta)
    return v


class ProspectTheory(LayerBase):
    layer_id = "l13"
    name = "Prospect Theory"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        # Fetch asset return or trade balance change data as gain/loss outcomes
        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.source IN ('fred', 'wdi')
              AND ds.series_id LIKE '%RETURN%'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 20:
            # Fallback: use GDP growth changes as gain/loss proxy
            rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = 'NY.GDP.MKTP.KD.ZG'
                ORDER BY dp.date
                """,
                (country,),
            )

        if not rows or len(rows) < 10:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        values = np.array([float(r["value"]) for r in rows])
        dates = [r["date"] for r in rows]

        # Compute period-over-period changes as gains/losses
        changes = np.diff(values)
        if len(changes) < 10:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient changes"}

        # --- 1. Estimate Value Function Parameters ---
        vf_params = self._estimate_value_function(changes)

        # --- 2. Loss Aversion Coefficient ---
        gains = changes[changes > 0]
        losses = changes[changes < 0]
        if len(gains) >= 3 and len(losses) >= 3:
            # Empirical loss aversion: ratio of mean absolute loss response
            # to mean gain response, scaled by frequency
            mean_gain = float(np.mean(gains))
            mean_loss = float(np.mean(np.abs(losses)))
            loss_aversion_empirical = mean_loss / mean_gain if mean_gain > 1e-10 else np.nan
        else:
            loss_aversion_empirical = np.nan

        # --- 3. Probability Weighting ---
        pw_params = self._estimate_probability_weighting(changes)

        # --- 4. Reference Point Effects ---
        ref_effects = self._reference_point_analysis(changes)

        # --- Score ---
        # High lambda (strong loss aversion) -> stress
        lam = vf_params["lambda"]
        lam_penalty = min(30, max(0, (lam - 1.0) * 15))

        # Probability distortion (gamma far from 1) -> stress
        gamma = pw_params["gamma"]
        gamma_penalty = min(25, abs(1.0 - gamma) * 50)

        # Asymmetry in reference effects -> stress
        asym = ref_effects["gain_loss_asymmetry"]
        asym_penalty = min(25, abs(asym) * 25)

        # Frequency of losses -> baseline stress
        loss_freq = len(losses) / len(changes) if len(changes) > 0 else 0.5
        freq_penalty = min(20, max(0, (loss_freq - 0.3) * 50))

        score = min(100, lam_penalty + gamma_penalty + asym_penalty + freq_penalty)

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(changes),
            "period": f"{dates[0]} to {dates[-1]}",
            "value_function": {
                "alpha": round(vf_params["alpha"], 4),
                "beta": round(vf_params["beta"], 4),
                "lambda": round(vf_params["lambda"], 4),
                "reference": "Tversky & Kahneman 1992: alpha=0.88, beta=0.88, lambda=2.25",
            },
            "loss_aversion": {
                "parametric_lambda": round(vf_params["lambda"], 4),
                "empirical_lambda": round(loss_aversion_empirical, 4)
                if np.isfinite(loss_aversion_empirical)
                else None,
                "n_gains": int(len(gains)),
                "n_losses": int(len(losses)),
            },
            "probability_weighting": {
                "gamma": round(pw_params["gamma"], 4),
                "overweight_small_p": pw_params["gamma"] < 1.0,
                "reference": "Prelec 1998, Tversky & Kahneman 1992: gamma ~ 0.61-0.69",
            },
            "reference_effects": ref_effects,
        }

    @staticmethod
    def _estimate_value_function(changes: np.ndarray) -> dict:
        """Estimate prospect theory value function parameters via MLE.

        Minimizes sum of squared deviations between ranked changes
        and the parametric value function, using the empirical CDF
        as a proxy for revealed preferences.
        """
        # Normalize to [0, 1] range for numerical stability
        max_abs = max(np.max(np.abs(changes)), 1e-10)
        normed = changes / max_abs

        def neg_loglik(params):
            alpha, beta, lam = params
            alpha = max(0.01, min(alpha, 1.5))
            beta = max(0.01, min(beta, 1.5))
            lam = max(0.5, min(lam, 5.0))
            v = _value_function(normed, alpha, beta, lam)
            # Rank-based loss: value function should preserve rank order
            ranks = np.argsort(np.argsort(normed)).astype(float)
            v_ranks = np.argsort(np.argsort(v)).astype(float)
            return float(np.sum((ranks - v_ranks) ** 2))

        result = minimize(
            neg_loglik,
            x0=[0.88, 0.88, 2.25],
            method="Nelder-Mead",
            options={"maxiter": 1000, "xatol": 1e-6, "fatol": 1e-6},
        )

        alpha, beta, lam = result.x
        alpha = max(0.01, min(float(alpha), 1.5))
        beta = max(0.01, min(float(beta), 1.5))
        lam = max(0.5, min(float(lam), 5.0))

        return {"alpha": alpha, "beta": beta, "lambda": lam}

    @staticmethod
    def _estimate_probability_weighting(changes: np.ndarray) -> dict:
        """Estimate Prelec probability weighting parameter gamma.

        Uses the empirical CDF of absolute changes and fits the
        Prelec weighting function to the probability-ranked outcomes.
        """
        n = len(changes)
        if n < 5:
            return {"gamma": 1.0}

        abs_changes = np.abs(changes)
        # Empirical CDF probabilities
        ranks = np.argsort(np.argsort(abs_changes)).astype(float)
        p_empirical = (ranks + 1) / (n + 1)

        # Under rational expectations, decision weights = probabilities
        # Under prospect theory, w(p) != p
        # Fit gamma by minimizing distance between weighted and empirical quantiles

        def loss(gamma_arr):
            gamma = max(0.2, min(float(gamma_arr[0]), 2.0))
            w = _prelec_weight(p_empirical, gamma)
            # Compare weighted quantile function to uniform
            uniform = np.linspace(0.01, 0.99, n)
            return float(np.sum((w - uniform) ** 2))

        result = minimize(
            loss,
            x0=[0.65],
            method="Nelder-Mead",
            options={"maxiter": 500},
        )

        gamma = max(0.2, min(float(result.x[0]), 2.0))
        return {"gamma": gamma}

    @staticmethod
    def _reference_point_analysis(changes: np.ndarray) -> dict:
        """Analyze reference-dependent behavior in outcome sequences.

        Tests for asymmetric responses around zero (the reference point)
        and adaptation effects (shifting reference points).
        """
        gains = changes[changes > 0]
        losses = changes[changes < 0]

        # Gain-loss asymmetry: compare dispersion
        gain_std = float(np.std(gains)) if len(gains) > 1 else 0
        loss_std = float(np.std(np.abs(losses))) if len(losses) > 1 else 0
        asymmetry = (loss_std - gain_std) / max(gain_std, 1e-10) if gain_std > 0 else 0

        # Reference point adaptation: test if recent changes shift
        # the implied reference (running mean acts as adaptive reference)
        n = len(changes)
        if n >= 10:
            window = max(3, n // 5)
            running_mean = np.convolve(changes, np.ones(window) / window, mode="valid")
            # Deviations from running reference
            trimmed = changes[window - 1 :]
            min_len = min(len(trimmed), len(running_mean))
            ref_adjusted = trimmed[:min_len] - running_mean[:min_len]
            adaptation_strength = float(1.0 - np.corrcoef(
                trimmed[:min_len], ref_adjusted
            )[0, 1]) if min_len > 2 else 0
        else:
            adaptation_strength = 0

        return {
            "gain_loss_asymmetry": round(asymmetry, 4),
            "gain_std": round(gain_std, 4),
            "loss_std": round(loss_std, 4),
            "adaptation_strength": round(float(adaptation_strength), 4),
        }
