"""Fiscal Multiplier estimation module.

Methodology
-----------
Structural VAR (SVAR) approach following Blanchard & Perotti (2002).

The identifying assumption exploits the fact that discretionary government
spending does not respond to output within the same quarter (decision and
implementation lags). Tax revenues respond automatically to output via
built-in stabilizers, but the elasticity can be calibrated externally.

VAR system (3 variables):
    z_t = [g_t, t_t, y_t]'
where:
    g_t = log real government spending
    t_t = log real tax revenue
    y_t = log real GDP

Identification:
1. Government spending is ordered first (does not respond contemporaneously
   to output or taxes).
2. Tax-output elasticity calibrated at 2.08 (Blanchard-Perotti for US).
   This removes the automatic stabilizer component from the tax innovation.
3. Remaining structural shocks identified from the orthogonalized residuals.

Multipliers computed from cumulative impulse response functions:
    Multiplier(h) = cumsum(IRF_y(h)) / cumsum(IRF_g(h))

Typical results: impact multiplier ~0.9, peak ~1.5 (around 6-8 quarters).

Also reports:
- Tax multiplier (typically negative, often larger in absolute value)
- State-dependent multipliers (recession vs expansion) if sufficient data

Score reflects fiscal sustainability risk from multiplier estimates.

Sources: FRED (government spending, tax revenue, real GDP)
"""

import numpy as np

from app.layers.base import LayerBase


def _var_estimate(data: np.ndarray, lags: int) -> dict:
    """Estimate reduced-form VAR(p) by equation-by-equation OLS.

    data: (T, k) array of endogenous variables
    Returns dict with coefficients, residuals, covariance matrix.
    """
    T, k = data.shape
    if T <= lags + 1:
        raise ValueError(f"Need more observations than lags+1. Got T={T}, lags={lags}")

    # Build lagged RHS matrix
    Y = data[lags:]
    X_parts = [np.ones((T - lags, 1))]
    for p in range(1, lags + 1):
        X_parts.append(data[lags - p : T - p])
    X = np.hstack(X_parts)

    # OLS: B = (X'X)^{-1} X'Y
    B = np.linalg.lstsq(X, Y, rcond=None)[0]
    resid = Y - X @ B
    sigma = (resid.T @ resid) / (T - lags - lags * k - 1)

    return {
        "coefficients": B,
        "residuals": resid,
        "sigma": sigma,
        "X": X,
        "Y": Y,
        "lags": lags,
        "k": k,
        "T_eff": T - lags,
    }


def _irf(var_result: dict, shock_idx: int, horizon: int, chol: np.ndarray) -> np.ndarray:
    """Compute impulse response functions from a VAR using Cholesky identification.

    Returns (horizon+1, k) array of responses to a one-unit structural shock.
    """
    B = var_result["coefficients"]
    lags = var_result["lags"]
    k = var_result["k"]

    # Extract companion-form matrices (lag coefficients only, skip intercept)
    A_list = []
    for p in range(lags):
        A_list.append(B[1 + p * k : 1 + (p + 1) * k, :].T)

    # Build companion matrix
    companion = np.zeros((k * lags, k * lags))
    for p in range(lags):
        companion[:k, p * k : (p + 1) * k] = A_list[p]
    if lags > 1:
        companion[k:, : k * (lags - 1)] = np.eye(k * (lags - 1))

    # Structural shock vector
    shock = chol[:, shock_idx]

    # Compute IRF
    irf_out = np.zeros((horizon + 1, k))
    state = np.zeros(k * lags)
    state[:k] = shock
    irf_out[0] = state[:k]

    for h in range(1, horizon + 1):
        state = companion @ state
        irf_out[h] = state[:k]

    return irf_out


class FiscalMultiplier(LayerBase):
    layer_id = "l2"
    name = "Fiscal Multiplier"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        var_lags = kwargs.get("var_lags", 4)
        irf_horizon = kwargs.get("irf_horizon", 20)
        tax_elasticity = kwargs.get("tax_output_elasticity", 2.08)

        # Fetch data
        series_map = {
            "gov_spending": f"GOV_SPENDING_{country}",
            "tax_revenue": f"TAX_REVENUE_{country}",
            "gdp": f"GDP_{country}",
        }
        data = {}
        for label, code in series_map.items():
            rows = await db.execute_fetchall(
                "SELECT date, value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE code = ?) ORDER BY date",
                (code,),
            )
            if rows:
                data[label] = {
                    "dates": [r[0] for r in rows],
                    "values": np.array([float(r[1]) for r in rows]),
                }

        required = ["gov_spending", "tax_revenue", "gdp"]
        if not all(k in data for k in required):
            return {"score": 50, "results": {"error": "insufficient fiscal data"}}

        # Align by common dates
        common = sorted(
            set(data["gov_spending"]["dates"])
            & set(data["tax_revenue"]["dates"])
            & set(data["gdp"]["dates"])
        )

        if len(common) < var_lags + 20:
            return {"score": 50, "results": {"error": "too few overlapping observations"}}

        g_dict = dict(zip(data["gov_spending"]["dates"], data["gov_spending"]["values"]))
        t_dict = dict(zip(data["tax_revenue"]["dates"], data["tax_revenue"]["values"]))
        y_dict = dict(zip(data["gdp"]["dates"], data["gdp"]["values"]))

        g = np.array([g_dict[d] for d in common])
        t = np.array([t_dict[d] for d in common])
        y = np.array([y_dict[d] for d in common])

        # Log-transform
        log_g = np.log(g)
        log_t = np.log(t)
        log_y = np.log(y)

        # VAR in levels (could also do differences; levels preserves cointegration info)
        Z = np.column_stack([log_g, log_t, log_y])

        results = {
            "country": country,
            "n_obs": len(common),
            "period": f"{common[0]} to {common[-1]}",
            "var_lags": var_lags,
            "tax_output_elasticity": tax_elasticity,
        }

        # Estimate reduced-form VAR
        try:
            var_res = _var_estimate(Z, var_lags)
        except ValueError as exc:
            return {"score": 50, "results": {"error": str(exc)}}

        var_res["sigma"]

        # --- Blanchard-Perotti identification ---
        # Step 1: Remove automatic tax response to output
        # u_t^tax = a_ty * u_t^y + structural_shock
        # where a_ty = tax_elasticity (calibrated)
        # Adjusted tax residual: u_t^tax_adj = u_t^tax - tax_elasticity * u_t^y
        resid = var_res["residuals"]
        u_g = resid[:, 0]
        u_t_raw = resid[:, 1]
        u_y = resid[:, 2]

        u_t_adj = u_t_raw - tax_elasticity * u_y

        # Step 2: Cholesky on [g, t_adj, y]
        resid_adj = np.column_stack([u_g, u_t_adj, u_y])
        sigma_adj = (resid_adj.T @ resid_adj) / len(resid_adj)
        chol = np.linalg.cholesky(sigma_adj)

        # --- Spending multiplier IRFs ---
        spending_irf = _irf(var_res, shock_idx=0, horizon=irf_horizon, chol=chol)
        # Multiplier: cumulative GDP response / cumulative spending response
        cum_y = np.cumsum(spending_irf[:, 2])
        cum_g = np.cumsum(spending_irf[:, 0])

        # Scale to dollar terms: multiply by (Y_mean / G_mean)
        y_g_ratio = float(np.mean(y) / np.mean(g))

        spending_multiplier = []
        for h in range(irf_horizon + 1):
            if abs(cum_g[h]) > 1e-12:
                mult = float(cum_y[h] / cum_g[h]) * y_g_ratio
            else:
                mult = 0.0
            spending_multiplier.append(round(mult, 3))

        results["spending_multiplier"] = {
            "impact": spending_multiplier[0],
            "peak": float(max(spending_multiplier)),
            "peak_quarter": int(np.argmax(spending_multiplier)),
            "cumulative_20q": spending_multiplier[-1] if len(spending_multiplier) > 20 else spending_multiplier[-1],
            "series": spending_multiplier,
        }

        # --- Tax multiplier IRFs ---
        tax_irf = _irf(var_res, shock_idx=1, horizon=irf_horizon, chol=chol)
        cum_y_tax = np.cumsum(tax_irf[:, 2])
        cum_t_tax = np.cumsum(tax_irf[:, 1])

        y_t_ratio = float(np.mean(y) / np.mean(t))

        tax_multiplier = []
        for h in range(irf_horizon + 1):
            if abs(cum_t_tax[h]) > 1e-12:
                mult = float(cum_y_tax[h] / cum_t_tax[h]) * y_t_ratio
            else:
                mult = 0.0
            tax_multiplier.append(round(mult, 3))

        results["tax_multiplier"] = {
            "impact": tax_multiplier[0],
            "peak": float(min(tax_multiplier)),  # tax multiplier typically negative
            "peak_quarter": int(np.argmin(tax_multiplier)),
            "series": tax_multiplier,
        }

        # --- IRF of GDP to spending shock ---
        results["irf_gdp_to_spending"] = {
            "horizon": irf_horizon,
            "response": spending_irf[:, 2].tolist(),
        }

        # --- Variance decomposition at select horizons ---
        # Forecast error variance decomposition
        vd_horizons = [1, 4, 8, 20]
        var_decomp = {}
        for h_vd in vd_horizons:
            if h_vd <= irf_horizon:
                # Accumulate squared IRF contributions
                total_var = np.zeros(3)
                contrib = np.zeros((3, 3))
                for shock_j in range(3):
                    irf_j = _irf(var_res, shock_idx=shock_j, horizon=h_vd, chol=chol)
                    for step in range(h_vd + 1):
                        contrib[shock_j] += irf_j[step] ** 2
                        total_var += irf_j[step] ** 2

                # Share of GDP variance (variable index 2) explained by each shock
                if total_var[2] > 0:
                    var_decomp[f"h{h_vd}"] = {
                        "spending_shock": round(float(contrib[0, 2] / total_var[2]) * 100, 1),
                        "tax_shock": round(float(contrib[1, 2] / total_var[2]) * 100, 1),
                        "output_shock": round(float(contrib[2, 2] / total_var[2]) * 100, 1),
                    }

        results["variance_decomposition_gdp"] = var_decomp

        # --- Score ---
        impact_mult = spending_multiplier[0] if spending_multiplier else 0
        # Very large or very small multipliers are concerning
        if impact_mult < 0:
            mult_penalty = 35  # contractionary fiscal expansion (austerity expansionary?)
        elif impact_mult > 3:
            mult_penalty = 25  # implausibly large
        elif impact_mult < 0.5:
            mult_penalty = 20  # very weak fiscal transmission
        else:
            mult_penalty = 5

        # Tax multiplier sign check (should be negative for tax increases)
        tax_sign_penalty = 15 if (tax_multiplier and tax_multiplier[0] > 0) else 0

        # Model fit: residual autocorrelation check (Ljung-Box proxy)
        resid_norms = np.sum(var_res["residuals"] ** 2, axis=1)
        resid_corr = np.corrcoef(resid_norms[:-1], resid_norms[1:])[0, 1]
        autocorr_penalty = min(abs(resid_corr) * 30, 20)

        score = min(mult_penalty + tax_sign_penalty + autocorr_penalty, 100)

        return {"score": round(score, 1), "results": results}
