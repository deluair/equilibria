"""Time Preference estimation module.

Estimates intertemporal choice parameters and their macroeconomic implications.

1. **Hyperbolic Discounting** (Laibson 1997):
   D(t) = 1 / (1 + k*t)
   where k is the discount rate. Hyperbolic discounters are impatient in
   the short run but patient over long horizons, generating dynamic
   inconsistency (preference reversals).

2. **Present Bias Measurement**:
   Compares short-run discount factor (beta) with long-run factor (delta).
   Present bias: beta < 1 implies disproportionate preference for
   immediate rewards. Estimated from consumption-savings patterns.

3. **Quasi-Hyperbolic (beta-delta) Calibration** (Laibson 1997):
   U = u(c_0) + beta * sum_{t=1}^T delta^t * u(c_t)
   Beta captures present bias, delta captures standard time preference.
   Joint estimation via Euler equation residuals.

4. **Retirement Savings Implications**:
   Simulates optimal vs actual savings rates under estimated beta-delta
   parameters. Welfare loss from present bias. Policy counterfactuals
   for auto-enrollment and matching contributions.

Score reflects undersaving risk: strong present bias (low beta) and
low savings rates -> high stress score.

Sources: FRED (savings rate, consumption, interest rates), WDI
"""

from __future__ import annotations

import numpy as np
from scipy.optimize import minimize, minimize_scalar

from app.layers.base import LayerBase


class TimePreference(LayerBase):
    layer_id = "l13"
    name = "Time Preference"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        # Fetch savings rate data
        savings_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('PSAVERT', 'NY.GNS.ICTR.ZS', 'SAVINGS_RATE')
            ORDER BY dp.date
            """,
            (country,),
        )

        # Fetch consumption growth
        consumption_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('DPCERL1Q225SBEA', 'NE.CON.PRVT.KD.ZG',
                                   'CONSUMPTION_GROWTH')
            ORDER BY dp.date
            """,
            (country,),
        )

        # Fetch interest rates
        rate_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('DGS10', 'FR.INR.RINR', 'REAL_RATE')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not savings_rows or len(savings_rows) < 10:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient savings data"}

        savings = np.array([float(r["value"]) for r in savings_rows])
        dates = [r["date"] for r in savings_rows]

        consumption = (
            np.array([float(r["value"]) for r in consumption_rows])
            if consumption_rows and len(consumption_rows) >= 5
            else None
        )
        rates = (
            np.array([float(r["value"]) for r in rate_rows])
            if rate_rows and len(rate_rows) >= 5
            else None
        )

        results = {
            "country": country,
            "n_obs": len(savings),
            "period": f"{dates[0]} to {dates[-1]}",
        }

        # --- 1. Hyperbolic Discounting ---
        hyp = self._estimate_hyperbolic(savings)
        results["hyperbolic_discounting"] = hyp

        # --- 2. Present Bias ---
        beta_delta = self._estimate_beta_delta(savings, consumption, rates)
        results["beta_delta"] = beta_delta

        # --- 3. Retirement Savings Implications ---
        retirement = self._retirement_implications(
            savings, beta_delta["beta"], beta_delta["delta"]
        )
        results["retirement_implications"] = retirement

        # --- Score ---
        # Strong present bias (low beta) -> high stress
        beta = beta_delta["beta"]
        beta_penalty = max(0, min(35, (1.0 - beta) * 70))

        # Low savings rate -> stress
        mean_savings = float(np.mean(savings))
        savings_penalty = max(0, min(25, (15.0 - mean_savings) * 2.5))

        # High hyperbolic k -> impatience -> stress
        k = hyp["k"]
        k_penalty = min(20, k * 20)

        # Welfare loss from present bias
        welfare_loss = retirement.get("welfare_loss_pct", 0) or 0
        welfare_penalty = min(20, welfare_loss * 2)

        score = min(100, beta_penalty + savings_penalty + k_penalty + welfare_penalty)

        return {"score": round(score, 1), **results}

    @staticmethod
    def _estimate_hyperbolic(savings: np.ndarray) -> dict:
        """Estimate hyperbolic discount rate k from savings behavior.

        D(t) = 1 / (1 + k*t)

        Uses the autocorrelation structure of savings to identify
        the degree of declining impatience (hyperbolic vs exponential).
        """
        n = len(savings)
        if n < 5:
            return {"k": 0.5, "note": "insufficient data, default used"}

        # Compute autocorrelations at multiple lags
        max_lag = min(12, n // 3)
        autocorrs = []
        for lag in range(1, max_lag + 1):
            if n - lag < 3:
                break
            corr = float(np.corrcoef(savings[:-lag], savings[lag:])[0, 1])
            if np.isfinite(corr):
                autocorrs.append((lag, corr))

        if len(autocorrs) < 3:
            return {"k": 0.5, "note": "insufficient lags for estimation"}

        lags = np.array([a[0] for a in autocorrs], dtype=float)
        acf = np.array([a[1] for a in autocorrs])

        # Fit hyperbolic: acf(t) ~ 1/(1+k*t) normalized
        # And exponential: acf(t) ~ delta^t
        # Compare fits

        def hyp_loss(k):
            k = max(0.01, float(k))
            predicted = 1.0 / (1.0 + k * lags)
            predicted = predicted * acf[0] / predicted[0] if predicted[0] > 0 else predicted
            return float(np.sum((acf - predicted) ** 2))

        result = minimize_scalar(hyp_loss, bounds=(0.01, 5.0), method="bounded")
        k_hat = max(0.01, float(result.x))

        # Exponential fit for comparison
        def exp_loss(delta):
            delta = max(0.01, min(0.999, float(delta)))
            predicted = delta ** lags
            predicted = predicted * acf[0] / predicted[0] if predicted[0] > 0 else predicted
            return float(np.sum((acf - predicted) ** 2))

        exp_result = minimize_scalar(exp_loss, bounds=(0.01, 0.999), method="bounded")

        hyp_fit = hyp_loss(k_hat)
        exp_fit = exp_loss(exp_result.x)

        return {
            "k": round(k_hat, 4),
            "hyperbolic_sse": round(hyp_fit, 6),
            "exponential_sse": round(exp_fit, 6),
            "prefers_hyperbolic": hyp_fit < exp_fit,
            "declining_impatience": k_hat > 0.1,
        }

    @staticmethod
    def _estimate_beta_delta(
        savings: np.ndarray,
        consumption: np.ndarray | None,
        rates: np.ndarray | None,
    ) -> dict:
        """Estimate quasi-hyperbolic (beta-delta) parameters.

        U = u(c_0) + beta * sum delta^t u(c_t)

        From the Euler equation under CRRA utility:
        E[delta_c_{t+1}/c_t] = beta * delta * (1 + r)

        With CRRA parameter sigma:
        E[(c_{t+1}/c_t)^(-sigma)] = 1 / (beta * delta * (1+r))
        """
        # Default parameters from Laibson (1997)
        beta_hat = 0.70
        delta_hat = 0.96

        if consumption is not None and rates is not None:
            min_len = min(len(consumption), len(rates)) - 1
            if min_len >= 5:
                # Consumption growth (if levels, compute growth)
                if np.mean(consumption) > 10:
                    # Likely levels, compute growth
                    c_growth = np.diff(np.log(np.maximum(consumption[:min_len + 1], 1e-10)))
                else:
                    c_growth = consumption[:min_len] / 100  # percentage to decimal

                r = rates[:min_len] / 100  # percentage to decimal

                # Joint estimation via GMM moment conditions
                # E[c_growth] = ln(beta*delta) + sigma*ln(1+r) under log utility (sigma=1)

                def gmm_loss(params):
                    beta, delta = params
                    beta = max(0.1, min(beta, 1.0))
                    delta = max(0.8, min(delta, 1.0))
                    implied = np.log(max(beta * delta, 1e-10)) + np.log(1 + r)
                    residuals = c_growth - implied
                    return float(np.mean(residuals**2))

                result = minimize(
                    gmm_loss,
                    x0=[0.70, 0.96],
                    method="Nelder-Mead",
                    options={"maxiter": 1000},
                )
                beta_hat, delta_hat = result.x
                beta_hat = max(0.1, min(float(beta_hat), 1.0))
                delta_hat = max(0.8, min(float(delta_hat), 1.0))
        else:
            # Estimate from savings pattern alone
            # Low/declining savings suggests present bias (low beta)
            mean_s = float(np.mean(savings))
            trend = np.polyfit(np.arange(len(savings)), savings, 1)[0]

            # Map savings rate to beta (higher savings -> higher beta)
            beta_hat = max(0.3, min(1.0, 0.5 + mean_s / 50))
            # Declining savings trend -> lower beta
            if trend < -0.1:
                beta_hat = max(0.3, beta_hat - 0.1)

        present_biased = beta_hat < 0.95

        return {
            "beta": round(beta_hat, 4),
            "delta": round(delta_hat, 4),
            "present_biased": present_biased,
            "effective_short_run_discount": round(beta_hat * delta_hat, 4),
            "reference": "Laibson 1997 calibration: beta=0.70, delta=0.96",
        }

    @staticmethod
    def _retirement_implications(
        savings: np.ndarray, beta: float, delta: float
    ) -> dict:
        """Simulate retirement savings under estimated beta-delta.

        Compares optimal savings (exponential discounter with delta only)
        vs actual savings under quasi-hyperbolic preferences.
        """
        current_savings_rate = float(savings[-1]) / 100

        # Optimal savings rate under exponential discounting (delta only)
        # From golden rule: s* = delta^(1/sigma) for log utility (sigma=1)
        optimal_rate = delta

        # Actual rate under quasi-hyperbolic (beta < 1 reduces savings)
        predicted_rate = beta * delta

        # Welfare loss from present bias (% of lifetime consumption)
        # Approximate: welfare loss proportional to (1-beta) * savings gap
        savings_gap = max(0, optimal_rate - current_savings_rate)
        welfare_loss = (1 - beta) * savings_gap * 100  # in percentage points

        # Years to retirement shortfall at current rate
        # Simplified: target replacement ratio of 70% over 30 years
        target_wealth = 15  # multiples of annual income
        years = 30
        if current_savings_rate > 0.001:
            # FV of savings at 5% real return
            r = 0.05
            accumulated = current_savings_rate * (((1 + r) ** years - 1) / r)
            shortfall = max(0, target_wealth - accumulated)
            shortfall_pct = shortfall / target_wealth * 100
        else:
            shortfall_pct = 100.0

        return {
            "mean_savings_rate": round(float(np.mean(savings)), 2),
            "current_savings_rate": round(float(savings[-1]), 2),
            "optimal_rate_exponential": round(optimal_rate * 100, 2),
            "predicted_rate_hyperbolic": round(predicted_rate * 100, 2),
            "welfare_loss_pct": round(welfare_loss, 2),
            "retirement_shortfall_pct": round(shortfall_pct, 1),
            "policy_implication": "auto-enrollment with escalation recommended"
            if beta < 0.85
            else "standard incentives likely sufficient",
        }
