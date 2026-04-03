"""Hazard model for unemployment duration.

Estimates Cox proportional hazards model for unemployment spell durations.
The hazard rate h(t|X) gives the instantaneous probability of exiting
unemployment at duration t, conditional on covariates X and survival to t.

Cox PH model (Cox 1972):
    h(t|X) = h_0(t) * exp(X*beta)

where h_0(t) is the baseline hazard (nonparametric) and exp(X*beta) is the
proportional shift due to covariates.

Key covariates:
    - Age, education, prior experience
    - UI benefit level and duration (moral hazard / liquidity effects)
    - Local labor market conditions (vacancy rate)
    - Industry of prior employment

Duration dependence:
    - Negative duration dependence: hazard falls with spell length
      (skill depreciation, stigma, discouragement)
    - Positive: learning about job search, benefit exhaustion spikes

The partial likelihood (Breslow 1974) avoids specifying h_0(t):
    L(beta) = prod_{i:uncensored} exp(X_i*beta) / sum_{j in R_i} exp(X_j*beta)

References:
    Cox, D. (1972). Regression Models and Life-Tables. JRSS-B 34(2): 187-220.
    Lancaster, T. (1979). Econometric Methods for the Duration of
        Unemployment. Econometrica 47(4): 939-956.
    Katz, L. & Meyer, B. (1990). The Impact of the Potential Duration of
        Unemployment Benefits on the Duration of Unemployment. Journal of
        Public Economics 41(1): 45-72.

Score: long average duration + negative duration dependence -> STRESS/CRISIS.
Short spells + rising hazard -> STABLE.
"""

import numpy as np

from app.layers.base import LayerBase


class UnemploymentDuration(LayerBase):
    layer_id = "l3"
    name = "Unemployment Duration (Hazard)"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        year = kwargs.get("year")

        year_clause = "AND dp.date = ?" if year else ""
        params = [country, "unemployment_duration"]
        if year:
            params.append(str(year))

        rows = await db.fetch_all(
            f"""
            SELECT dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = ?
              {year_clause}
            ORDER BY dp.date DESC
            """,
            tuple(params),
        )

        if not rows or len(rows) < 30:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient spell data"}

        import json

        durations = []
        censored = []
        covariates = []

        for row in rows:
            dur = row["value"]
            if dur is None or dur <= 0:
                continue
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            cens = int(meta.get("censored", 0))
            age = meta.get("age")
            educ = meta.get("years_schooling")
            ui_benefit = meta.get("ui_benefit_level", 0)
            if age is None or educ is None:
                continue

            durations.append(float(dur))
            censored.append(cens)
            covariates.append([float(age), float(educ), float(ui_benefit)])

        n = len(durations)
        if n < 30:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient valid obs"}

        t = np.array(durations)
        d = np.array(censored)  # 0 = event observed, 1 = right censored
        X = np.array(covariates)
        event = 1 - d  # 1 = exited unemployment

        # Cox PH via Newton-Raphson on partial likelihood
        beta, se, log_pl = self._cox_ph(t, event, X)

        # Kaplan-Meier survival curve for baseline
        km_times, km_survival = self._kaplan_meier(t, event)

        # Duration dependence: test if hazard changes with duration
        # Add log(duration) as covariate and test its coefficient
        X_dur = np.column_stack([X, np.log(t)])
        beta_dur, se_dur, _ = self._cox_ph(t, event, X_dur)
        duration_dep_coef = float(beta_dur[-1])
        duration_dep_se = float(se_dur[-1])
        duration_dep_z = duration_dep_coef / duration_dep_se if abs(duration_dep_se) > 1e-10 else 0.0

        # Summary statistics
        median_duration = float(np.median(t))
        mean_duration = float(np.mean(t))
        pct_long_term = float(np.mean(t > 26))  # >26 weeks = long-term

        # Score: high avg duration + negative duration dependence -> stress
        if mean_duration > 40:
            score = 70.0 + min(30.0, (mean_duration - 40) * 0.5)
        elif mean_duration > 20:
            score = 35.0 + (mean_duration - 20) * 1.75
        else:
            score = mean_duration * 1.75
        # Amplify if strong negative duration dependence
        if duration_dep_coef < 0 and abs(duration_dep_z) > 1.96:
            score = min(100.0, score * 1.2)
        score = max(0.0, min(100.0, score))

        coef_names = ["age", "years_schooling", "ui_benefit_level"]

        return {
            "score": round(score, 2),
            "country": country,
            "n_spells": n,
            "n_completed": int(np.sum(event)),
            "n_censored": int(np.sum(d)),
            "duration_stats": {
                "mean_weeks": round(mean_duration, 1),
                "median_weeks": round(median_duration, 1),
                "pct_long_term_26wk": round(pct_long_term * 100, 1),
            },
            "cox_ph": {
                "coefficients": dict(zip(coef_names, beta.tolist())),
                "std_errors": dict(zip(coef_names, se.tolist())),
                "hazard_ratios": dict(zip(coef_names, np.exp(beta).tolist())),
                "log_partial_likelihood": round(log_pl, 4),
            },
            "duration_dependence": {
                "coefficient": round(duration_dep_coef, 4),
                "std_error": round(duration_dep_se, 4),
                "z_statistic": round(duration_dep_z, 2),
                "negative_dependence": duration_dep_coef < 0 and abs(duration_dep_z) > 1.96,
            },
            "survival_quantiles": {
                "25th_pct_weeks": round(float(np.percentile(t, 25)), 1),
                "50th_pct_weeks": round(float(np.percentile(t, 50)), 1),
                "75th_pct_weeks": round(float(np.percentile(t, 75)), 1),
            },
        }

    @staticmethod
    def _cox_ph(t: np.ndarray, event: np.ndarray, X: np.ndarray,
                max_iter: int = 50, tol: float = 1e-8):
        """Cox proportional hazards via Newton-Raphson on partial likelihood."""
        n, k = X.shape
        beta = np.zeros(k)

        # Sort by time (descending for risk set computation)
        order = np.argsort(-t)
        t[order]
        event_s = event[order]
        X_s = X[order]

        for iteration in range(max_iter):
            eta = X_s @ beta
            exp_eta = np.exp(eta - np.max(eta))  # numerical stability

            # Cumulative sums for risk set (reverse cumsum since sorted descending)
            # At each failure time, risk set includes all with t >= t_i
            risk_sum = np.cumsum(exp_eta)
            risk_X_sum = np.cumsum(X_s * exp_eta[:, None], axis=0)

            # Gradient and Hessian of partial log-likelihood
            grad = np.zeros(k)
            hess = np.zeros((k, k))
            log_pl = 0.0

            for i in range(n):
                if event_s[i] == 0:
                    continue
                s0 = risk_sum[i]
                s1 = risk_X_sum[i]
                if s0 < 1e-20:
                    continue
                x_bar = s1 / s0
                grad += X_s[i] - x_bar
                log_pl += eta[i] - np.log(s0)

                # Hessian contribution
                risk_X2_sum_i = np.zeros((k, k))
                for j in range(i + 1):
                    risk_X2_sum_i += np.outer(X_s[j], X_s[j]) * exp_eta[j]
                hess -= risk_X2_sum_i / s0 - np.outer(x_bar, x_bar)

            # Newton step
            try:
                step = np.linalg.solve(hess, grad)
            except np.linalg.LinAlgError:
                step = np.linalg.lstsq(hess, grad, rcond=None)[0]

            beta_new = beta - step
            if np.max(np.abs(beta_new - beta)) < tol:
                beta = beta_new
                break
            beta = beta_new

        # Standard errors from observed information matrix
        se = np.sqrt(np.maximum(np.diag(np.linalg.pinv(-hess)), 0.0))

        return beta, se, float(log_pl)

    @staticmethod
    def _kaplan_meier(t: np.ndarray, event: np.ndarray):
        """Kaplan-Meier survival estimator."""
        order = np.argsort(t)
        t_s = t[order]
        e_s = event[order]

        unique_times = np.unique(t_s[e_s == 1])
        survival = 1.0
        times = [0.0]
        surv_vals = [1.0]

        n_at_risk = len(t_s)
        idx = 0
        for ut in unique_times:
            # Count events and censored before this time
            while idx < len(t_s) and t_s[idx] < ut:
                if e_s[idx] == 0:
                    n_at_risk -= 1
                idx += 1
            d_i = np.sum((t_s == ut) & (e_s == 1))
            if n_at_risk > 0:
                survival *= (1.0 - d_i / n_at_risk)
            n_at_risk -= np.sum(t_s == ut)
            times.append(float(ut))
            surv_vals.append(survival)

        return np.array(times), np.array(surv_vals)
