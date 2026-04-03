"""Phillips Curve estimation module.

Methodology
-----------
Three Phillips Curve specifications:

1. **Traditional Phillips Curve** (Phillips 1958):
   pi_t = alpha + beta * u_t + e_t
   Tests the inverse inflation-unemployment relationship.

2. **Expectations-Augmented Phillips Curve** (Friedman 1968, Phelps 1967):
   pi_t = pi^e_t + beta * (u_t - u*) + e_t
   Uses lagged inflation as adaptive expectations proxy.
   Allows estimation of NAIRU (u*) via iterative NLS or grid search.

3. **New Keynesian Phillips Curve** (Gali & Gertler 1999):
   pi_t = beta * E[pi_{t+1}] + kappa * x_t + e_t
   where x_t is the output gap or real marginal cost.
   Estimated via GMM with lagged instruments.

NAIRU estimation via:
- Staiger-Stock-Watson (1997) approach: grid search over u* to minimize
  sum of squared residuals in the expectations-augmented specification.
- Confidence band via Fieller method.

Score reflects how well the Phillips Curve fits (low R-squared or unstable
parameters -> higher stress score, indicating breakdown of the relationship).

Sources: FRED (CPI inflation, unemployment rate, output gap)
"""

import numpy as np
from scipy import optimize

from app.layers.base import LayerBase


def _ols(y: np.ndarray, X: np.ndarray) -> dict:
    """Ordinary least squares with heteroskedasticity-robust (HC1) standard errors."""
    n, k = X.shape
    beta = np.linalg.lstsq(X, y, rcond=None)[0]
    resid = y - X @ beta
    sse = float(np.sum(resid ** 2))
    sst = float(np.sum((y - np.mean(y)) ** 2))
    r_squared = 1 - sse / sst if sst > 0 else 0.0

    # HC1 robust standard errors
    bread = np.linalg.inv(X.T @ X)
    meat = X.T @ np.diag(resid ** 2) @ X
    vcov = (n / (n - k)) * bread @ meat @ bread
    se = np.sqrt(np.diag(vcov))
    t_stats = beta / se

    return {
        "beta": beta,
        "se": se,
        "t_stats": t_stats,
        "r_squared": r_squared,
        "residuals": resid,
        "n": n,
        "k": k,
    }


class PhillipsCurve(LayerBase):
    layer_id = "l2"
    name = "Phillips Curve"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")

        # Fetch inflation and unemployment
        inf_rows = await db.execute_fetchall(
            "SELECT date, value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE code = ?) ORDER BY date",
            (f"INFLATION_{country}",),
        )
        unemp_rows = await db.execute_fetchall(
            "SELECT date, value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE code = ?) ORDER BY date",
            (f"UNEMP_{country}",),
        )

        if not inf_rows or not unemp_rows:
            return {"score": 50, "results": {"error": "insufficient data"}}

        # Align series by date
        inf_dict = {r[0]: float(r[1]) for r in inf_rows}
        unemp_dict = {r[0]: float(r[1]) for r in unemp_rows}
        common_dates = sorted(set(inf_dict) & set(unemp_dict))

        if len(common_dates) < 10:
            return {"score": 50, "results": {"error": "too few overlapping observations"}}

        pi = np.array([inf_dict[d] for d in common_dates])
        u = np.array([unemp_dict[d] for d in common_dates])

        results = {"country": country, "n_obs": len(common_dates)}
        results["period"] = f"{common_dates[0]} to {common_dates[-1]}"

        # --- 1. Traditional Phillips Curve ---
        X_trad = np.column_stack([np.ones(len(u)), u])
        trad = _ols(pi, X_trad)
        results["traditional"] = {
            "intercept": float(trad["beta"][0]),
            "slope": float(trad["beta"][1]),
            "slope_se": float(trad["se"][1]),
            "slope_t": float(trad["t_stats"][1]),
            "r_squared": round(trad["r_squared"], 4),
        }

        # --- 2. Expectations-Augmented Phillips Curve ---
        # pi_t - pi_{t-1} = alpha + beta * u_t + e_t
        # => acceleration form
        pi_accel = np.diff(pi)
        u_aug = u[1:]
        X_aug = np.column_stack([np.ones(len(u_aug)), u_aug])
        aug = _ols(pi_accel, X_aug)
        results["expectations_augmented"] = {
            "intercept": float(aug["beta"][0]),
            "slope": float(aug["beta"][1]),
            "slope_se": float(aug["se"][1]),
            "slope_t": float(aug["t_stats"][1]),
            "r_squared": round(aug["r_squared"], 4),
        }

        # --- NAIRU estimation via grid search ---
        u_grid = np.linspace(float(np.min(u_aug)), float(np.max(u_aug)), 200)
        best_ssr = np.inf
        nairu_est = float(np.mean(u_aug))

        for u_star in u_grid:
            gap = u_aug - u_star
            X_nairu = np.column_stack([np.ones(len(gap)), gap])
            beta_hat = np.linalg.lstsq(X_nairu, pi_accel, rcond=None)[0]
            resid = pi_accel - X_nairu @ beta_hat
            ssr = float(np.sum(resid ** 2))
            if ssr < best_ssr:
                best_ssr = ssr
                nairu_est = float(u_star)

        # NAIRU confidence interval (delta method approximation)
        nairu_se = float(aug["se"][0] / abs(aug["beta"][1])) if abs(aug["beta"][1]) > 1e-10 else np.nan
        results["nairu"] = {
            "estimate": round(nairu_est, 2),
            "se": round(nairu_se, 2) if np.isfinite(nairu_se) else None,
            "ci_90": [
                round(nairu_est - 1.645 * nairu_se, 2) if np.isfinite(nairu_se) else None,
                round(nairu_est + 1.645 * nairu_se, 2) if np.isfinite(nairu_se) else None,
            ],
        }

        # --- 3. New Keynesian Phillips Curve (reduced form, GMM-style) ---
        # pi_t = gamma_f * pi_{t+1} + gamma_b * pi_{t-1} + kappa * gap_t + e_t
        # Fetch output gap if available
        gap_rows = await db.execute_fetchall(
            "SELECT date, value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE code = ?) ORDER BY date",
            (f"OUTPUT_GAP_{country}",),
        )

        if gap_rows:
            gap_dict = {r[0]: float(r[1]) for r in gap_rows}
            nkpc_dates = sorted(set(common_dates) & set(gap_dict))

            if len(nkpc_dates) >= 10:
                pi_nk = np.array([inf_dict[d] for d in nkpc_dates])
                gap_nk = np.array([gap_dict[d] for d in nkpc_dates])

                # Hybrid NKPC: pi_t = gamma_f * pi_{t+1} + gamma_b * pi_{t-1} + kappa * x_t
                # Use 2-period interior to get leads and lags
                pi_lead = pi_nk[2:]
                pi_lag = pi_nk[:-2]
                pi_curr = pi_nk[1:-1]
                gap_curr = gap_nk[1:-1]

                # IV estimation: instrument pi_{t+1} with pi_{t-2}, gap_{t-1}
                # Simplified: OLS on reduced form for signal extraction
                X_nk = np.column_stack([np.ones(len(pi_curr)), pi_lag, gap_curr])
                nk_ols = _ols(pi_curr, X_nk)

                # Estimate forward-looking component via 2SLS
                # First stage: regress pi_{t+1} on instruments (pi_{t-1}, pi_{t-2}, gap_{t-1})
                if len(pi_nk) > 4:
                    pi_lead_2 = pi_nk[3:]
                    min_n = min(len(pi_lead_2), len(pi_nk[:-3]), len(pi_nk[1:-2]), len(gap_nk[1:-2]))
                    if min_n >= 6:
                        dep = pi_nk[2 : 2 + min_n]
                        instruments = np.column_stack([
                            np.ones(min_n),
                            pi_nk[:min_n],        # pi_{t-2}
                            pi_nk[1 : 1 + min_n], # pi_{t-1}
                            gap_nk[:min_n],        # gap_{t-2}
                        ])
                        pi_fwd = pi_nk[3 : 3 + min_n]
                        gap_mid = gap_nk[2 : 2 + min_n]

                        # First stage
                        pi_fwd_hat = instruments @ np.linalg.lstsq(instruments, pi_fwd, rcond=None)[0]

                        # Second stage
                        X_2sls = np.column_stack([np.ones(min_n), pi_fwd_hat, pi_nk[1 : 1 + min_n], gap_mid])
                        nk_2sls = _ols(dep, X_2sls)

                        results["nkpc"] = {
                            "gamma_forward": float(nk_2sls["beta"][1]),
                            "gamma_backward": float(nk_2sls["beta"][2]),
                            "kappa": float(nk_2sls["beta"][3]),
                            "gamma_forward_se": float(nk_2sls["se"][1]),
                            "kappa_se": float(nk_2sls["se"][3]),
                            "r_squared": round(nk_2sls["r_squared"], 4),
                            "n": min_n,
                            "method": "2SLS",
                        }

        if "nkpc" not in results:
            results["nkpc"] = {"note": "output gap data unavailable, NKPC not estimated"}

        # --- Score ---
        # Low R-squared in traditional PC -> relationship breaking down -> stress
        r2_trad = trad["r_squared"]
        # Positive slope = perverse (stagflation risk)
        slope_sign_penalty = 20 if trad["beta"][1] > 0 else 0
        # Low fit -> unstable relationship
        fit_penalty = (1 - max(r2_trad, 0)) * 40
        # NAIRU far from current unemployment
        current_u = float(u[-1])
        nairu_gap = abs(current_u - nairu_est)
        gap_penalty = min(nairu_gap * 5, 30)

        score = min(fit_penalty + slope_sign_penalty + gap_penalty, 100)

        return {"score": round(score, 1), "results": results}
