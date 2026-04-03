"""Twin Deficits hypothesis testing module.

Methodology
-----------
The twin deficits hypothesis posits that a fiscal deficit (government
budget deficit) leads to a current account deficit through two channels:

1. **Mundell-Fleming channel**: Fiscal expansion -> higher interest rates ->
   capital inflows -> currency appreciation -> trade deficit.

2. **Absorption channel**: Fiscal expansion -> higher aggregate demand ->
   increased imports -> trade deficit.

The Ricardian Equivalence counter-argument (Barro 1974) suggests that
rational agents offset government borrowing with increased private saving,
nullifying the twin deficit link.

Empirical tests:

1. **Granger causality tests** (Granger 1969):
   - Does fiscal deficit Granger-cause current account deficit?
   - Does current account deficit Granger-cause fiscal deficit?
   - Bidirectional feedback?
   Lag selection via BIC on VAR(p).

2. **Bivariate VAR analysis**:
   z_t = [fiscal_balance_t, current_account_t]'
   Impulse response functions show dynamic transmission.

3. **Correlation analysis**:
   - Contemporaneous correlation
   - Cross-correlation function at various leads/lags
   - Rolling correlation for time-varying relationship

4. **Panel analysis** (if multi-country data available):
   CA_it = alpha_i + beta * FB_it + gamma * X_it + e_it
   with country fixed effects and controls.

Score reflects the degree of twin deficit vulnerability.

Sources: FRED, IMF WEO/IFS, WDI
"""

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


def _bic_var_order(y: np.ndarray, max_lags: int = 8) -> int:
    """Select VAR lag order by BIC. y is (T, k)."""
    T, k = y.shape
    best_bic = np.inf
    best_p = 1

    for p in range(1, min(max_lags + 1, T // 3)):
        Y = y[p:]
        n = len(Y)
        X_parts = [np.ones((n, 1))]
        for lag in range(1, p + 1):
            X_parts.append(y[p - lag : T - lag])
        X = np.hstack(X_parts)

        B = np.linalg.lstsq(X, Y, rcond=None)[0]
        resid = Y - X @ B
        sigma = (resid.T @ resid) / n
        log_det = np.log(np.linalg.det(sigma) + 1e-20)
        n_params = k * (1 + k * p)
        bic = log_det + n_params * np.log(n) / n

        if bic < best_bic:
            best_bic = bic
            best_p = p

    return best_p


def _granger_test(y: np.ndarray, x: np.ndarray, max_lags: int = 8) -> dict:
    """Granger causality test: does x Granger-cause y?

    Uses F-test comparing restricted (own lags only) vs unrestricted (own + x lags) models.
    Lag order selected by BIC on unrestricted model.
    """
    T = len(y)

    # Select lag order
    Z = np.column_stack([y, x])
    p = _bic_var_order(Z, max_lags)

    # Unrestricted model: y_t = c + sum(a_j * y_{t-j}) + sum(b_j * x_{t-j}) + e_t
    n = T - p
    Y = y[p:]
    X_unr_parts = [np.ones((n, 1))]
    for j in range(1, p + 1):
        X_unr_parts.append(y[p - j : T - j].reshape(-1, 1))
        X_unr_parts.append(x[p - j : T - j].reshape(-1, 1))
    X_unr = np.hstack(X_unr_parts)

    beta_unr = np.linalg.lstsq(X_unr, Y, rcond=None)[0]
    resid_unr = Y - X_unr @ beta_unr
    sse_unr = float(np.sum(resid_unr ** 2))

    # Restricted model: y_t = c + sum(a_j * y_{t-j}) + e_t
    X_res_parts = [np.ones((n, 1))]
    for j in range(1, p + 1):
        X_res_parts.append(y[p - j : T - j].reshape(-1, 1))
    X_res = np.hstack(X_res_parts)

    beta_res = np.linalg.lstsq(X_res, Y, rcond=None)[0]
    resid_res = Y - X_res @ beta_res
    sse_res = float(np.sum(resid_res ** 2))

    # F-test
    k_unr = X_unr.shape[1]
    k_res = X_res.shape[1]
    df_num = k_unr - k_res  # = p (number of x lags)
    df_den = n - k_unr

    if sse_unr > 0 and df_den > 0:
        f_stat = ((sse_res - sse_unr) / df_num) / (sse_unr / df_den)
        p_value = 1 - stats.f.cdf(f_stat, df_num, df_den)
    else:
        f_stat = 0.0
        p_value = 1.0

    return {
        "lags": p,
        "f_statistic": round(float(f_stat), 3),
        "p_value": round(float(p_value), 4),
        "rejects_null_5pct": float(p_value) < 0.05,
        "rejects_null_10pct": float(p_value) < 0.10,
    }


class TwinDeficits(LayerBase):
    layer_id = "l2"
    name = "Twin Deficits"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        max_lags = kwargs.get("max_lags", 8)
        irf_horizon = kwargs.get("irf_horizon", 20)

        # Fetch fiscal balance and current account as % of GDP
        fb_rows = await db.execute_fetchall(
            "SELECT date, value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE code = ?) ORDER BY date",
            (f"FISCAL_BAL_GDP_{country}",),
        )
        ca_rows = await db.execute_fetchall(
            "SELECT date, value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE code = ?) ORDER BY date",
            (f"CURR_ACCT_GDP_{country}",),
        )

        if not fb_rows or not ca_rows:
            return {"score": 50, "results": {"error": "insufficient data"}}

        fb_dict = {r[0]: float(r[1]) for r in fb_rows}
        ca_dict = {r[0]: float(r[1]) for r in ca_rows}
        common = sorted(set(fb_dict) & set(ca_dict))

        if len(common) < 15:
            return {"score": 50, "results": {"error": "too few overlapping observations"}}

        fb = np.array([fb_dict[d] for d in common])
        ca = np.array([ca_dict[d] for d in common])

        results = {
            "country": country,
            "n_obs": len(common),
            "period": f"{common[0]} to {common[-1]}",
        }

        # --- Contemporaneous correlation ---
        corr, corr_p = stats.pearsonr(fb, ca)
        results["correlation"] = {
            "pearson_r": round(float(corr), 4),
            "p_value": round(float(corr_p), 4),
            "significant_5pct": float(corr_p) < 0.05,
            "interpretation": (
                "Positive" if corr > 0 else "Negative"
            ) + f" correlation ({corr:.3f}). "
            + (
                "Consistent with twin deficits hypothesis."
                if corr > 0.2
                else "Weak or inconsistent with twin deficits."
            ),
        }

        # --- Cross-correlation function ---
        max_cross_lag = min(12, len(common) // 4)
        cross_corr = {}
        for lag in range(-max_cross_lag, max_cross_lag + 1):
            if lag < 0:
                r_val = float(np.corrcoef(fb[:lag], ca[-lag:])[0, 1])
            elif lag > 0:
                r_val = float(np.corrcoef(fb[lag:], ca[:-lag])[0, 1])
            else:
                r_val = float(np.corrcoef(fb, ca)[0, 1])
            cross_corr[lag] = round(r_val, 4)

        peak_lag = max(cross_corr, key=cross_corr.get)
        results["cross_correlation"] = {
            "values": cross_corr,
            "peak_lag": peak_lag,
            "peak_value": cross_corr[peak_lag],
            "fb_leads": peak_lag > 0,
        }

        # --- Granger causality tests ---
        gc_fb_to_ca = _granger_test(ca, fb, max_lags)
        gc_ca_to_fb = _granger_test(fb, ca, max_lags)

        if gc_fb_to_ca["rejects_null_5pct"] and gc_ca_to_fb["rejects_null_5pct"]:
            direction = "bidirectional"
        elif gc_fb_to_ca["rejects_null_5pct"]:
            direction = "fiscal -> current account"
        elif gc_ca_to_fb["rejects_null_5pct"]:
            direction = "current account -> fiscal (reverse)"
        else:
            direction = "no significant Granger causality"

        results["granger_causality"] = {
            "fiscal_to_current_account": gc_fb_to_ca,
            "current_account_to_fiscal": gc_ca_to_fb,
            "direction": direction,
        }

        # --- VAR analysis with IRFs ---
        Z = np.column_stack([fb, ca])
        p = _bic_var_order(Z, max_lags)

        T = len(fb)
        n = T - p
        Y_var = Z[p:]
        X_parts = [np.ones((n, 1))]
        for lag in range(1, p + 1):
            X_parts.append(Z[p - lag : T - lag])
        X_var = np.hstack(X_parts)

        B_var = np.linalg.lstsq(X_var, Y_var, rcond=None)[0]
        resid_var = Y_var - X_var @ B_var
        sigma_var = (resid_var.T @ resid_var) / n

        # Cholesky: order fiscal balance first (fiscal shock identified)
        chol = np.linalg.cholesky(sigma_var)

        # Extract lag coefficient matrices
        A_list = []
        for lag in range(p):
            A_list.append(B_var[1 + lag * 2 : 1 + (lag + 1) * 2, :].T)

        # Companion form
        k = 2
        companion = np.zeros((k * p, k * p))
        for lag in range(p):
            companion[:k, lag * k : (lag + 1) * k] = A_list[lag]
        if p > 1:
            companion[k:, : k * (p - 1)] = np.eye(k * (p - 1))

        # IRF: fiscal shock -> current account
        irf_fb_shock = np.zeros((irf_horizon + 1, k))
        state = np.zeros(k * p)
        state[:k] = chol[:, 0]  # fiscal shock
        irf_fb_shock[0] = state[:k]
        for h in range(1, irf_horizon + 1):
            state = companion @ state
            irf_fb_shock[h] = state[:k]

        results["var_irf"] = {
            "var_lags": p,
            "fiscal_shock_to_ca": {
                "response": irf_fb_shock[:, 1].tolist(),
                "cumulative": np.cumsum(irf_fb_shock[:, 1]).tolist(),
                "peak_response": float(np.max(np.abs(irf_fb_shock[:, 1]))),
                "peak_quarter": int(np.argmax(np.abs(irf_fb_shock[:, 1]))),
            },
            "fiscal_shock_to_fb": {
                "response": irf_fb_shock[:, 0].tolist(),
            },
            "horizon": irf_horizon,
        }

        # --- Rolling correlation ---
        window = kwargs.get("rolling_window", 20)
        if len(fb) >= window + 5:
            rolling_corr = []
            rolling_dates = []
            for i in range(len(fb) - window + 1):
                r_roll = float(np.corrcoef(fb[i : i + window], ca[i : i + window])[0, 1])
                rolling_corr.append(round(r_roll, 4))
                rolling_dates.append(common[i + window - 1])

            results["rolling_correlation"] = {
                "window": window,
                "values": rolling_corr,
                "dates": rolling_dates,
                "mean": round(float(np.mean(rolling_corr)), 4),
                "currently_positive": rolling_corr[-1] > 0 if rolling_corr else None,
            }

        # --- Current state ---
        both_deficit = fb[-1] < 0 and ca[-1] < 0
        results["current_state"] = {
            "fiscal_balance_latest": round(float(fb[-1]), 2),
            "current_account_latest": round(float(ca[-1]), 2),
            "twin_deficits_present": both_deficit,
            "fiscal_deficit": fb[-1] < 0,
            "current_account_deficit": ca[-1] < 0,
        }

        # --- Score ---
        # Twin deficits present
        twin_penalty = 25 if both_deficit else 5

        # Large deficits
        fb_penalty = min(abs(float(fb[-1])) * 3, 20) if fb[-1] < 0 else 0
        ca_penalty = min(abs(float(ca[-1])) * 3, 20) if ca[-1] < 0 else 0

        # Granger causality confirmed (fiscal -> CA)
        gc_penalty = 15 if gc_fb_to_ca["rejects_null_5pct"] else 0

        # Strong positive correlation
        corr_penalty = 10 if corr > 0.3 else 0

        score = min(twin_penalty + fb_penalty + ca_penalty + gc_penalty + corr_penalty, 100)

        return {"score": round(score, 1), "results": results}
