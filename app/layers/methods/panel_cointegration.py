"""Panel cointegration: Pedroni tests, FMOLS/DOLS, Pesaran CD, common correlated effects.

Methodology
-----------
**Pedroni Panel Cointegration Tests** (Pedroni 1999, 2004):
Under H0: no cointegration, panel residuals are I(1).
Seven statistics grouped into two classes:

    Within-dimension (panel statistics):
        panel-v:   large positive -> reject H0
        panel-rho: small negative -> reject H0
        panel-PP:  small negative (Phillips-Perron)
        panel-ADF: small negative (Augmented Dickey-Fuller)

    Between-dimension (group mean statistics):
        group-rho, group-PP, group-ADF

    Asymptotic distribution: N(0,1) (or chi-sq for panel-v)
    Adjusted statistics: (Z - mu_N * sqrt(N)) / (sigma_N * sqrt(N))
    mu_N, sigma_N from Pedroni (1999) Table 2 response surface estimates

**FMOLS (Fully Modified OLS)** (Phillips & Hansen 1990, panel extension):
Corrects for serial correlation and endogeneity in cointegrated systems:
    beta_FMOLS = (sum_i X_i'X_i)^{-1} * sum_i (X_i' y_i^+ - T * lambda_i^+)

where y_i^+ are modified y using long-run covariance estimates.

**DOLS (Dynamic OLS)** (Saikkonen 1991, panel Kao & Chiang 2000):
    y_it = alpha_i + beta * x_it + sum_{j=-p}^{p} delta_j * Delta x_{i,t+j} + eps_it
Leads/lags of Delta x control for endogeneity. Pooled estimator averages
over i.

**Cross-Section Dependence (Pesaran CD test)** (Pesaran 2004):
    CD = sqrt(2T / (N*(N-1))) * sum_{i<j} rho_ij
    rho_ij = T^{-1} sum_t (e_it * e_jt) / (s_i * s_j)
    Under H0: CD ~ N(0,1). Large |CD| indicates cross-section dependence.

**Common Correlated Effects (CCE)** (Pesaran 2006):
Filters cross-section averages from the model to remove unobserved
common factors. CCE estimator robust to general forms of cross-section
dependence. Pooled CCE (CCEP) and mean group CCE (CCEMG).

Score: no cointegration + strong cross-section dependence + heterogeneous
coef -> STRESS (spurious inference risk). Cointegration + low CD -> STABLE.

References:
    Pedroni, P. (1999). Critical Values for Cointegration Tests in
        Heterogeneous Panels. Oxford Bulletin of Economics 61: 653-670.
    Pedroni, P. (2004). Panel Cointegration: Asymptotic and Finite Sample
        Properties. Econometric Theory 20(3): 597-625.
    Kao, C. & Chiang, M. (2000). On the Estimation and Inference of a
        Cointegrated Regression in Panel Data. Advances in Econometrics 15.
    Pesaran, M. H. (2004). General Diagnostic Tests for Cross Section
        Dependence in Panels. Cambridge Working Paper in Economics 435.
    Pesaran, M. H. (2006). Estimation and Inference in Large Heterogeneous
        Panels with a Multifactor Error Structure. Econometrica 74(4): 967-1012.
"""

import json

import numpy as np
from scipy.stats import norm

from app.layers.base import LayerBase


def _adf_stat(y: np.ndarray, lags: int = 1) -> float:
    """ADF t-statistic for H0: unit root."""
    n = len(y)
    if n < lags + 5:
        return 0.0
    dy = np.diff(y)
    nlag = min(lags, len(dy) - 2)
    if nlag < 1:
        nlag = 1
    # Build regressor matrix: y_{t-1} and lags of dy
    T = len(dy) - nlag
    if T < 5:
        return 0.0
    Y = dy[nlag:]
    X = np.column_stack([
        y[nlag: nlag + T],
        *[dy[nlag - j - 1: nlag - j - 1 + T] for j in range(nlag)],
        np.ones(T),
    ])
    try:
        beta, resid, _, _ = np.linalg.lstsq(X, Y, rcond=None)
    except np.linalg.LinAlgError:
        return 0.0
    pred = X @ beta
    e = Y - pred
    s2 = float(np.sum(e ** 2)) / max(T - X.shape[1], 1)
    XtX_inv = np.linalg.pinv(X.T @ X)
    se_beta0 = np.sqrt(s2 * XtX_inv[0, 0]) if s2 > 0 else 1e-10
    return float(beta[0]) / se_beta0 if se_beta0 > 0 else 0.0


def _long_run_variance(e: np.ndarray, bandwidth: int | None = None) -> float:
    """Newey-West long-run variance estimate."""
    n = len(e)
    if n < 4:
        return float(np.var(e))
    if bandwidth is None:
        bandwidth = max(1, int(4 * (n / 100) ** (2 / 9)))
    gamma0 = float(np.sum(e ** 2)) / n
    lrv = gamma0
    for k in range(1, bandwidth + 1):
        w = 1 - k / (bandwidth + 1)
        gamma_k = float(np.sum(e[k:] * e[:n - k])) / n
        lrv += 2 * w * gamma_k
    return max(lrv, 1e-10)


class PanelCointegration(LayerBase):
    layer_id = "l18"
    name = "Panel Cointegration"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.value, dp.date, ds.metadata, ds.country_iso3
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source = 'panel_cointegration'
            ORDER BY ds.country_iso3, dp.date
            """,
        )

        if not rows or len(rows) < 30:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        # Parse panel data: { country: { date: {y, x} } }
        panel: dict[str, dict] = {}
        for row in rows:
            iso = row["country_iso3"]
            if not iso:
                continue
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            panel.setdefault(iso, {})[row["date"]] = {
                "y": float(row["value"]) if row["value"] is not None else None,
                "x": float(meta.get("x", 0.0)),
            }

        N = len(panel)
        if N < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "need >= 3 cross-sections"}

        # Build arrays per unit
        units = []
        for iso, time_data in panel.items():
            dates = sorted(d for d, v in time_data.items() if v["y"] is not None)
            if len(dates) < 15:
                continue
            y = np.array([time_data[d]["y"] for d in dates])
            x = np.array([time_data[d]["x"] for d in dates])
            units.append({"id": iso, "y": y, "x": x, "T": len(dates)})

        N = len(units)
        if N < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient units after filter"}

        # OLS residuals per unit
        for u in units:
            y, x, T = u["y"], u["x"], u["T"]
            X = np.column_stack([np.ones(T), x])
            try:
                beta = np.linalg.lstsq(X, y, rcond=None)[0]
                u["resid"] = y - X @ beta
                u["beta"] = beta
            except np.linalg.LinAlgError:
                u["resid"] = y - np.mean(y)
                u["beta"] = np.array([np.mean(y), 0.0])

        # --- Pedroni Tests ---
        # Response surface mu and sigma from Pedroni (1999) for k=1 regressor
        mu_rho = -1.0
        sigma_rho = 0.5
        adf_stats = []
        rho_stats = []

        for u in units:
            e = u["resid"]
            T = u["T"]
            # ADF stat on residuals
            adf_t = _adf_stat(e, lags=1)
            adf_stats.append(adf_t)
            # Rho stat: T*(rho_hat - 1) where rho_hat from AR(1)
            if T > 5:
                rho_hat = float(np.corrcoef(e[1:], e[:-1])[0, 1])
                rho_stats.append(T * (rho_hat - 1))

        # Group-mean panel ADF: average of unit ADF stats
        group_adf = float(np.mean(adf_stats)) if adf_stats else 0.0
        group_rho = float(np.mean(rho_stats)) if rho_stats else 0.0

        # Standardized panel-ADF stat (asymptotic N(0,1))
        mu_adf = -1.73  # Pedroni (1999) Table 2, k=1, no trend
        sigma_adf = 0.745
        panel_adf_z = (group_adf - np.sqrt(N) * mu_adf) / (sigma_adf * np.sqrt(N)) if N > 0 else 0
        panel_rho_z = (group_rho - np.sqrt(N) * mu_rho) / (sigma_rho * np.sqrt(N)) if N > 0 else 0

        # p-values: for ADF/rho, left-tail (reject for negative values)
        p_adf = float(norm.cdf(panel_adf_z))
        p_rho = float(norm.cdf(panel_rho_z))
        cointegrated = (p_adf < 0.10) or (p_rho < 0.10)

        pedroni_results = {
            "panel_adf_statistic": round(panel_adf_z, 4),
            "panel_adf_pvalue": round(p_adf, 4),
            "panel_rho_statistic": round(panel_rho_z, 4),
            "panel_rho_pvalue": round(p_rho, 4),
            "group_mean_adf": round(float(group_adf), 4),
            "group_mean_rho": round(float(group_rho), 4),
            "cointegrated": cointegrated,
            "n_units": N,
        }

        # --- FMOLS pooled estimator ---
        fmols_betas = []
        for u in units:
            y, x, T = u["y"], u["x"], u["T"]
            e = u["resid"]
            dx = np.diff(x)
            # Long-run covariance
            omega_11 = _long_run_variance(e)
            if len(dx) > 2:
                cov_ex = np.cov(e[1:], dx)[0, 1] if len(e[1:]) == len(dx) else 0.0
                omega_12 = float(np.mean(e[1:] * dx)) if len(dx) > 0 else 0.0
            else:
                cov_ex = 0.0
                omega_12 = 0.0
            # FM correction
            y_plus = y - (omega_12 / omega_11) * np.concatenate([[0.0], np.diff(y)])
            X_m = np.column_stack([np.ones(T), x])
            try:
                beta_fm = np.linalg.lstsq(X_m, y_plus, rcond=None)[0]
                fmols_betas.append(beta_fm[1])
            except np.linalg.LinAlgError:
                pass

        fmols_beta = float(np.mean(fmols_betas)) if fmols_betas else None
        fmols_se = float(np.std(fmols_betas) / np.sqrt(len(fmols_betas))) if len(fmols_betas) > 1 else None

        fmols_results = {}
        if fmols_beta is not None:
            fmols_results["pooled_beta"] = round(fmols_beta, 4)
            if fmols_se is not None:
                fmols_results["se"] = round(fmols_se, 4)
                fmols_results["t_stat"] = round(fmols_beta / fmols_se, 4) if fmols_se > 0 else None
                t = fmols_beta / fmols_se if fmols_se > 0 else 0
                fmols_results["p_value"] = round(2 * float(norm.sf(abs(t))), 4)

        # --- DOLS pooled estimator (1 lead/lag) ---
        dols_betas = []
        for u in units:
            y, x, T = u["y"], u["x"], u["T"]
            if T < 8:
                continue
            dx = np.diff(x)
            # Add 1 lead and 1 lag of dx
            # Interior observations: t=1 to T-2
            T_d = T - 2
            if T_d < 5:
                continue
            y_d = y[1:T - 1]
            x_d = x[1:T - 1]
            dx_lag = dx[:T_d]
            dx_lead = dx[1:T_d + 1] if len(dx) >= T_d + 1 else dx[1:len(dx)]
            min_len = min(len(y_d), len(x_d), len(dx_lag), len(dx_lead))
            if min_len < 5:
                continue
            X_d = np.column_stack([
                np.ones(min_len),
                x_d[:min_len],
                dx_lag[:min_len],
                dx_lead[:min_len],
            ])
            try:
                beta_d = np.linalg.lstsq(X_d, y_d[:min_len], rcond=None)[0]
                dols_betas.append(beta_d[1])
            except np.linalg.LinAlgError:
                pass

        dols_beta = float(np.mean(dols_betas)) if dols_betas else None

        dols_results = {}
        if dols_beta is not None:
            dols_se = float(np.std(dols_betas) / np.sqrt(len(dols_betas))) if len(dols_betas) > 1 else None
            dols_results["pooled_beta"] = round(dols_beta, 4)
            if dols_se is not None and dols_se > 0:
                dols_results["se"] = round(dols_se, 4)
                t = dols_beta / dols_se
                dols_results["t_stat"] = round(float(t), 4)
                dols_results["p_value"] = round(2 * float(norm.sf(abs(t))), 4)

        # --- Pesaran CD test ---
        residuals_matrix = []
        for u in units:
            e = u["resid"]
            residuals_matrix.append(e)
        min_T = min(len(e) for e in residuals_matrix)
        residuals_matrix = [e[:min_T] for e in residuals_matrix]
        N = len(residuals_matrix)
        cd_sum = 0.0
        n_pairs = 0
        for i in range(N):
            for j in range(i + 1, N):
                e_i = residuals_matrix[i]
                e_j = residuals_matrix[j]
                sd_i = float(np.std(e_i))
                sd_j = float(np.std(e_j))
                if sd_i > 0 and sd_j > 0:
                    rho_ij = float(np.mean(e_i * e_j)) / (sd_i * sd_j)
                    cd_sum += rho_ij
                    n_pairs += 1

        if n_pairs > 0:
            cd_stat = float(np.sqrt(2 * min_T / (N * (N - 1))) * cd_sum)
            cd_pvalue = 2 * float(norm.sf(abs(cd_stat)))
            pesaran_cd = {
                "cd_statistic": round(cd_stat, 4),
                "p_value": round(cd_pvalue, 4),
                "cross_section_dependent": cd_pvalue < 0.05,
                "n_pairs": n_pairs,
            }
        else:
            pesaran_cd = {"cd_statistic": None, "error": "insufficient pairs"}

        # --- CCE mean-group estimator ---
        cce_betas = []
        y_bar = np.mean([u["y"][:min_T] for u in units], axis=0)
        x_bar = np.mean([u["x"][:min_T] for u in units], axis=0)
        for u in units:
            T_u = min(u["T"], min_T)
            y_u = u["y"][:T_u]
            x_u = u["x"][:T_u]
            y_b = y_bar[:T_u]
            x_b = x_bar[:T_u]
            # CCE: augment with cross-section means
            X_cce = np.column_stack([np.ones(T_u), x_u, y_b, x_b])
            try:
                beta_cce = np.linalg.lstsq(X_cce, y_u, rcond=None)[0]
                cce_betas.append(beta_cce[1])  # coefficient on x
            except np.linalg.LinAlgError:
                pass

        cce_results = {}
        if cce_betas:
            cce_mg_beta = float(np.mean(cce_betas))
            cce_mg_se = float(np.std(cce_betas) / np.sqrt(len(cce_betas))) if len(cce_betas) > 1 else None
            cce_results["mg_beta"] = round(cce_mg_beta, 4)
            if cce_mg_se is not None and cce_mg_se > 0:
                cce_results["mg_se"] = round(cce_mg_se, 4)
                t = cce_mg_beta / cce_mg_se
                cce_results["t_stat"] = round(float(t), 4)
                cce_results["p_value"] = round(2 * float(norm.sf(abs(t))), 4)
            # Heterogeneity: Hausman-style FMOLS vs CCE comparison
            if fmols_beta is not None:
                cce_results["heterogeneity_diff"] = round(abs(cce_mg_beta - fmols_beta), 4)

        # --- Score ---
        score = 30.0

        # No cointegration -> higher score (spurious inference risk)
        if not cointegrated:
            score += 30
        else:
            score -= 10

        # Cross-section dependence (ignoring it biases tests)
        cd_info = pesaran_cd
        if cd_info.get("cross_section_dependent"):
            score += 15

        # Coefficient heterogeneity
        if fmols_beta is not None and dols_beta is not None:
            diff = abs(fmols_beta - dols_beta)
            if diff > 0.2:
                score += min(diff * 20, 15)

        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "n_units": N,
            "pedroni_tests": pedroni_results,
            "fmols": fmols_results,
            "dols": dols_results,
            "pesaran_cd": pesaran_cd,
            "cce_mean_group": cce_results,
        }
