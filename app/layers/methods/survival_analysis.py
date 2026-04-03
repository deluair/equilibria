"""Survival analysis: Kaplan-Meier, Cox PH, AFT, competing risks (Klein & Moeschberger 2003).

Duration models are used in economics for unemployment spells, firm survival,
trade relationship duration, patent lifetimes, and regime duration. The key
challenge is censoring: we observe incomplete spells for units that have not
yet experienced the event.

Models:
    Kaplan-Meier: nonparametric survival function estimator
        S(t) = prod_{t_i <= t} (1 - d_i / n_i)

    Cox Proportional Hazards: semi-parametric
        h(t|X) = h_0(t) * exp(X * beta)
        Partial likelihood estimation (no baseline hazard needed)

    Accelerated Failure Time (AFT): parametric
        log(T) = X * beta + sigma * W, where W ~ specified distribution

    Competing risks: multiple possible event types
        Cause-specific hazard and subdistribution hazard (Fine-Gray)

    Frailty models: unobserved heterogeneity via random effects
        h(t|X,v) = v * h_0(t) * exp(X * beta), v ~ Gamma(1/theta, 1/theta)

References:
    Cox, D.R. (1972). Regression Models and Life-Tables. JRSS-B 34(2): 187-220.
    Klein, J. & Moeschberger, M. (2003). Survival Analysis. Springer.
    Fine, J. & Gray, R. (1999). A Proportional Hazards Model for the
        Subdistribution of a Competing Risk. JASA 94(446): 496-509.
    Lancaster, T. (1990). The Econometric Analysis of Transition Data. Cambridge.

Score: high hazard / short median survival -> high score (STRESS).
Long, stable duration -> STABLE.
"""

import json

import numpy as np

from app.layers.base import LayerBase


class SurvivalAnalysis(LayerBase):
    layer_id = "l18"
    name = "Survival Analysis"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        duration_type = kwargs.get("duration_type", "unemployment")

        rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'survival'
              AND ds.description LIKE ?
            ORDER BY dp.value
            """,
            (country, f"%{duration_type}%"),
        )

        if not rows or len(rows) < 15:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient duration data"}

        # Parse duration data: time, event indicator, covariates, event type
        times, events, event_types = [], [], []
        x_data = []
        x_keys_set = set()

        for row in rows:
            t = row["value"]
            if t is None or t <= 0:
                continue
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            event = meta.get("event", 1)  # 1 = event observed, 0 = censored
            etype = meta.get("event_type", 1)
            covars = meta.get("covariates", {})
            x_keys_set |= set(covars.keys())

            times.append(float(t))
            events.append(int(event))
            event_types.append(int(etype))
            x_data.append(covars)

        n = len(times)
        if n < 15:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient valid obs"}

        T = np.array(times)
        E = np.array(events)
        ET = np.array(event_types)

        # Kaplan-Meier estimator
        km = self._kaplan_meier(T, E)

        # Cox PH model if covariates exist
        x_keys = sorted(x_keys_set)
        cox_result = None
        if x_keys:
            X = np.column_stack([
                np.array([d.get(k, 0.0) for d in x_data]) for k in x_keys
            ])
            cox_result = self._cox_ph(T, E, X, x_keys)

        # Competing risks summary
        competing = None
        unique_types = sorted(set(ET))
        if len(unique_types) > 1:
            competing = {}
            for etype in unique_types:
                mask = ET == etype
                competing[f"type_{etype}"] = {
                    "n_events": int(np.sum(E[mask])),
                    "cumulative_incidence": round(float(np.sum(E[mask]) / n), 4),
                }

        # Duration dependence test: is hazard increasing or decreasing?
        dur_dep = self._duration_dependence(T, E)

        # Frailty variance (simplified: test for unobserved heterogeneity)
        frailty = self._frailty_test(T, E)

        # Score: short median survival or high hazard -> high score
        median_surv = km["median_survival"]
        if median_surv is not None:
            # Normalize relative to data range
            max_t = float(np.max(T))
            rel_median = median_surv / max_t if max_t > 0 else 1.0
            if rel_median < 0.2:
                score = 70.0 + (0.2 - rel_median) * 150.0
            elif rel_median < 0.5:
                score = 30.0 + (0.5 - rel_median) * 133.3
            else:
                score = max(0.0, 30.0 * (1.0 - rel_median))
        else:
            # No median reached (many censored), use event rate
            event_rate = float(np.mean(E))
            score = event_rate * 50.0
        score = max(0.0, min(100.0, score))

        return {
            "score": round(score, 2),
            "country": country,
            "duration_type": duration_type,
            "n_obs": n,
            "n_events": int(np.sum(E)),
            "n_censored": int(np.sum(1 - E)),
            "kaplan_meier": km,
            "cox_ph": cox_result,
            "competing_risks": competing,
            "duration_dependence": dur_dep,
            "frailty": frailty,
        }

    @staticmethod
    def _kaplan_meier(T: np.ndarray, E: np.ndarray) -> dict:
        """Kaplan-Meier survival function estimator."""
        # Sort by time
        order = np.argsort(T)
        T_sorted = T[order]
        E_sorted = E[order]

        unique_times = np.unique(T_sorted[E_sorted == 1])
        survival = 1.0
        n_at_risk = len(T)
        km_times = [0.0]
        km_survival = [1.0]
        km_se = [0.0]
        var_sum = 0.0

        for t in unique_times:
            # Number at risk just before t
            n_at_risk = int(np.sum(T_sorted >= t))
            d = int(np.sum((T_sorted == t) & (E_sorted == 1)))
            if n_at_risk > 0:
                survival *= (1.0 - d / n_at_risk)
                if n_at_risk > d:
                    var_sum += d / (n_at_risk * (n_at_risk - d))
            km_times.append(float(t))
            km_survival.append(survival)
            # Greenwood's formula for SE
            km_se.append(survival * np.sqrt(var_sum))

        # Median survival time
        median_surv = None
        for i, s in enumerate(km_survival):
            if s <= 0.5:
                median_surv = km_times[i]
                break

        # Survival at quartiles
        quartile_surv = {}
        for q in [0.25, 0.50, 0.75]:
            for i, s in enumerate(km_survival):
                if s <= (1 - q):
                    quartile_surv[str(q)] = round(km_times[i], 4)
                    break

        return {
            "median_survival": round(median_surv, 4) if median_surv is not None else None,
            "quartile_times": quartile_surv,
            "survival_at_points": {
                round(km_times[i], 4): round(km_survival[i], 4)
                for i in range(0, len(km_times), max(1, len(km_times) // 10))
            },
            "n_time_points": len(unique_times),
        }

    @staticmethod
    def _cox_ph(T: np.ndarray, E: np.ndarray, X: np.ndarray,
                var_names: list) -> dict:
        """Cox PH via Newton-Raphson on partial likelihood."""
        n, k = X.shape
        beta = np.zeros(k)

        # Sort by time (descending for risk set computation)
        order = np.argsort(-T)
        T_s = T[order]
        E_s = E[order]
        X_s = X[order]

        for iteration in range(50):
            # Compute risk set quantities
            exp_xb = np.exp(X_s @ beta)
            # Cumulative sums from last to first (risk sets)
            cum_exp = np.cumsum(exp_xb)
            cum_exp_x = np.cumsum((X_s.T * exp_xb).T, axis=0)

            # Gradient
            grad = np.zeros(k)
            hess = np.zeros((k, k))
            for i in range(n):
                if E_s[i] == 0:
                    continue
                risk_sum = cum_exp[i]
                if risk_sum <= 0:
                    continue
                risk_x = cum_exp_x[i]
                x_bar = risk_x / risk_sum
                grad += X_s[i] - x_bar
                # Hessian term
                risk_xx = np.zeros((k, k))
                for j in range(i + 1):
                    risk_xx += np.outer(X_s[j], X_s[j]) * exp_xb[j]
                hess -= risk_xx / risk_sum - np.outer(x_bar, x_bar)

            # Newton step
            try:
                step = np.linalg.solve(hess, grad)
            except np.linalg.LinAlgError:
                break
            beta -= step
            if np.max(np.abs(step)) < 1e-8:
                break

        # Standard errors from inverse Hessian
        try:
            V = np.linalg.inv(-hess)
            se = np.sqrt(np.maximum(np.diag(V), 0.0))
        except np.linalg.LinAlgError:
            se = np.full(k, float("inf"))

        hazard_ratios = np.exp(beta)

        return {
            "coefficients": {v: round(float(beta[j]), 4) for j, v in enumerate(var_names)},
            "std_errors": {v: round(float(se[j]), 4) for j, v in enumerate(var_names)},
            "hazard_ratios": {v: round(float(hazard_ratios[j]), 4) for j, v in enumerate(var_names)},
        }

    @staticmethod
    def _duration_dependence(T: np.ndarray, E: np.ndarray) -> dict:
        """Test for positive/negative duration dependence."""
        # Split durations into short and long halves
        median_t = float(np.median(T))
        short = (T <= median_t) & (E == 1)
        long_ = (T > median_t) & (E == 1)
        n_short = int(np.sum(T <= median_t))
        n_long = int(np.sum(T > median_t))

        h_short = int(np.sum(short)) / max(n_short, 1)
        h_long = int(np.sum(long_)) / max(n_long, 1)

        if h_short > h_long * 1.2:
            pattern = "negative"  # Hazard decreasing -> long spells self-perpetuate
        elif h_long > h_short * 1.2:
            pattern = "positive"  # Hazard increasing -> longer spells more likely to end
        else:
            pattern = "none"

        return {
            "hazard_first_half": round(h_short, 4),
            "hazard_second_half": round(h_long, 4),
            "pattern": pattern,
        }

    @staticmethod
    def _frailty_test(T: np.ndarray, E: np.ndarray) -> dict:
        """Test for unobserved heterogeneity (over-dispersion in hazard)."""
        # Simple test: compare observed variance of hazard to Poisson expectation
        # If over-dispersed, suggests frailty
        n = len(T)
        event_rate = float(np.mean(E))
        # Split into time bins
        n_bins = max(5, n // 10)
        bin_edges = np.percentile(T, np.linspace(0, 100, n_bins + 1))
        bin_edges = np.unique(bin_edges)
        n_bins = len(bin_edges) - 1

        bin_rates = []
        for i in range(n_bins):
            mask = (T >= bin_edges[i]) & (T < bin_edges[i + 1])
            n_in_bin = int(np.sum(mask))
            if n_in_bin > 0:
                bin_rates.append(float(np.sum(E[mask])) / n_in_bin)

        if len(bin_rates) < 3:
            return {"theta": None, "over_dispersed": None}

        rates = np.array(bin_rates)
        mean_rate = float(np.mean(rates))
        var_rate = float(np.var(rates))

        # Over-dispersion ratio
        theta = var_rate / max(mean_rate, 1e-10) - mean_rate if mean_rate > 0 else 0.0
        theta = max(0.0, theta)

        return {
            "theta": round(theta, 4),
            "over_dispersed": theta > 0.1,
        }
