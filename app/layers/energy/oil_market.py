"""Oil market analysis: structural VAR, pass-through, SPR, OPEC market power.

Methodology
-----------
**Kilian (2009) structural VAR** decomposes oil price changes into three
orthogonal shocks using sign restrictions on a 3-variable VAR:

    z_t = [delta_prod_t, rea_t, rpo_t]'

where:
    delta_prod_t = change in global crude oil production
    rea_t        = real economic activity index (dry cargo shipping rates)
    rpo_t        = real price of oil (WTI deflated by US CPI)

Identification via Cholesky ordering (recursive):
    1. Oil supply shock: production ordered first (slow to adjust)
    2. Aggregate demand shock: global activity responds within period
    3. Oil-specific demand shock: residual (precautionary/speculative)

**Oil price pass-through to CPI** uses distributed lag regression:
    delta_cpi_t = alpha + sum_{j=0}^{k} beta_j * delta_oil_{t-j} + eps_t

**Strategic Petroleum Reserve (SPR)**: measures reserve adequacy as
days of net import cover and estimates price dampening effect of releases.

**OPEC market power**: Lerner index proxy from price-cost margin,
Herfindahl-Hirschman index of OPEC production shares.

Score reflects oil market stress: high price volatility, large supply
shocks, and weak SPR coverage increase the score.

Sources: EIA, FRED, IMF commodity prices
"""

import numpy as np

from app.layers.base import LayerBase


def _var_estimate(data: np.ndarray, lags: int) -> dict:
    """Estimate reduced-form VAR(p) by equation-by-equation OLS."""
    T, k = data.shape
    if T <= lags + 1:
        raise ValueError(f"Need T > lags+1. Got T={T}, lags={lags}")

    Y = data[lags:]
    X_parts = [np.ones((T - lags, 1))]
    for p in range(1, lags + 1):
        X_parts.append(data[lags - p : T - p])
    X = np.hstack(X_parts)

    B = np.linalg.lstsq(X, Y, rcond=None)[0]
    resid = Y - X @ B
    n_eff = T - lags
    sigma = (resid.T @ resid) / n_eff

    return {"coefficients": B, "residuals": resid, "sigma": sigma, "lags": lags, "k": k, "T_eff": n_eff}


def _companion_irf(var_result: dict, chol: np.ndarray, shock_idx: int, horizon: int) -> np.ndarray:
    """Compute IRF from companion form with Cholesky identification."""
    B = var_result["coefficients"]
    lags = var_result["lags"]
    k = var_result["k"]

    A_list = []
    for p in range(lags):
        A_list.append(B[1 + p * k : 1 + (p + 1) * k, :].T)

    companion = np.zeros((k * lags, k * lags))
    for p in range(lags):
        companion[:k, p * k : (p + 1) * k] = A_list[p]
    if lags > 1:
        companion[k:, : k * (lags - 1)] = np.eye(k * (lags - 1))

    irf_out = np.zeros((horizon + 1, k))
    state = np.zeros(k * lags)
    state[:k] = chol[:, shock_idx]
    irf_out[0] = state[:k]
    for h in range(1, horizon + 1):
        state = companion @ state
        irf_out[h] = state[:k]

    return irf_out


class OilMarket(LayerBase):
    layer_id = "l16"
    name = "Oil Market"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        var_lags = kwargs.get("var_lags", 12)
        irf_horizon = kwargs.get("irf_horizon", 24)
        pt_lags = kwargs.get("passthrough_lags", 6)

        series_map = {
            "oil_production": "OIL_PRODUCTION_GLOBAL",
            "economic_activity": "REAL_ECONOMIC_ACTIVITY",
            "oil_price": "OIL_PRICE_WTI",
            "cpi": f"CPI_{country}",
            "spr": f"SPR_{country}",
            "net_imports": f"OIL_NET_IMPORTS_{country}",
            "opec_production": "OPEC_PRODUCTION",
        }
        data = {}
        for label, code in series_map.items():
            rows = await db.execute_fetchall(
                "SELECT date, value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE code = ?) ORDER BY date",
                (code,),
            )
            if rows:
                data[label] = {r[0]: float(r[1]) for r in rows}

        results = {"country": country}

        # --- Kilian structural VAR ---
        var_vars = ["oil_production", "economic_activity", "oil_price"]
        if all(v in data for v in var_vars):
            common = sorted(set.intersection(*[set(data[v]) for v in var_vars]))

            if len(common) > var_lags + 30:
                Z = np.column_stack([
                    np.diff(np.array([data["oil_production"][d] for d in common])),
                    np.array([data["economic_activity"][d] for d in common[1:]]),
                    np.log(np.array([data["oil_price"][d] for d in common[1:]])),
                ])

                try:
                    var_res = _var_estimate(Z, var_lags)
                    chol = np.linalg.cholesky(var_res["sigma"])

                    shock_labels = ["supply", "aggregate_demand", "oil_specific_demand"]
                    decomposition = {}
                    for idx, label in enumerate(shock_labels):
                        irf = _companion_irf(var_res, chol, idx, irf_horizon)
                        cum_price = np.cumsum(irf[:, 2]).tolist()
                        decomposition[label] = {
                            "price_response": [round(float(v), 4) for v in cum_price],
                            "impact": round(float(irf[0, 2]), 4),
                            "cumulative_12m": round(float(cum_price[min(12, len(cum_price) - 1)]), 4),
                        }

                    # Variance decomposition at 12-month horizon
                    vd_h = min(12, irf_horizon)
                    total_var = np.zeros(3)
                    contrib = np.zeros((3, 3))
                    for shock_j in range(3):
                        irf_j = _companion_irf(var_res, chol, shock_j, vd_h)
                        for step in range(vd_h + 1):
                            contrib[shock_j] += irf_j[step] ** 2
                            total_var += irf_j[step] ** 2

                    if total_var[2] > 0:
                        decomposition["variance_decomposition_12m"] = {
                            sl: round(float(contrib[i, 2] / total_var[2]) * 100, 1)
                            for i, sl in enumerate(shock_labels)
                        }

                    results["kilian_svar"] = decomposition
                except np.linalg.LinAlgError:
                    results["kilian_svar"] = {"error": "Cholesky decomposition failed"}

        # --- Oil price pass-through to CPI ---
        if "oil_price" in data and "cpi" in data:
            common_pt = sorted(set(data["oil_price"]) & set(data["cpi"]))

            if len(common_pt) > pt_lags + 20:
                oil = np.log(np.array([data["oil_price"][d] for d in common_pt]))
                cpi = np.log(np.array([data["cpi"][d] for d in common_pt]))
                d_oil = np.diff(oil)
                d_cpi = np.diff(cpi)

                effective_n = len(d_oil) - pt_lags
                if effective_n >= 15:
                    Y_pt = d_cpi[pt_lags:]
                    X_parts = [np.ones((effective_n, 1))]
                    for lag in range(pt_lags + 1):
                        X_parts.append(d_oil[pt_lags - lag : len(d_oil) - lag].reshape(-1, 1))
                    X_pt = np.hstack(X_parts)

                    beta = np.linalg.lstsq(X_pt, Y_pt, rcond=None)[0]
                    resid = Y_pt - X_pt @ beta
                    sse = float(np.sum(resid ** 2))
                    sst = float(np.sum((Y_pt - np.mean(Y_pt)) ** 2))
                    r_sq = 1 - sse / sst if sst > 0 else 0.0

                    # HC1 standard errors
                    k_p = X_pt.shape[1]
                    bread = np.linalg.inv(X_pt.T @ X_pt)
                    meat = X_pt.T @ np.diag(resid ** 2) @ X_pt
                    vcov = (effective_n / (effective_n - k_p)) * bread @ meat @ bread
                    se = np.sqrt(np.diag(vcov))

                    pt_coeffs = beta[1 : pt_lags + 2]
                    short_run = float(pt_coeffs[0])
                    long_run = float(np.sum(pt_coeffs))

                    results["oil_cpi_passthrough"] = {
                        "short_run": round(short_run, 4),
                        "long_run": round(long_run, 4),
                        "short_run_se": round(float(se[1]), 4),
                        "lag_coefficients": [round(float(c), 4) for c in pt_coeffs],
                        "r_squared": round(r_sq, 4),
                        "n_obs": effective_n,
                    }

        # --- Strategic Petroleum Reserve ---
        if "spr" in data and "net_imports" in data:
            common_spr = sorted(set(data["spr"]) & set(data["net_imports"]))
            if common_spr:
                latest = common_spr[-1]
                spr_barrels = data["spr"][latest]
                daily_imports = data["net_imports"][latest]
                days_cover = spr_barrels / daily_imports if daily_imports > 0 else float("inf")

                spr_vals = np.array([data["spr"][d] for d in common_spr])
                spr_change = float(spr_vals[-1] - spr_vals[0]) / spr_vals[0] * 100 if spr_vals[0] > 0 else 0

                results["spr"] = {
                    "reserve_barrels": spr_barrels,
                    "days_of_import_cover": round(days_cover, 1),
                    "iea_minimum_90_days": days_cover >= 90,
                    "change_pct": round(spr_change, 1),
                    "latest_date": latest,
                }

        # --- OPEC market power ---
        if "opec_production" in data and "oil_production" in data:
            common_opec = sorted(set(data["opec_production"]) & set(data["oil_production"]))
            if len(common_opec) >= 5:
                opec_vals = np.array([data["opec_production"][d] for d in common_opec])
                global_vals = np.array([data["oil_production"][d] for d in common_opec])
                market_share = opec_vals / global_vals
                latest_share = float(market_share[-1])

                # Lerner index proxy: (P - MC) / P approximated by inverse supply elasticity
                oil_prices = np.array([data["oil_price"][d] for d in common_opec
                                       if d in data.get("oil_price", {})])
                if len(oil_prices) > 10:
                    price_vol = float(np.std(np.diff(np.log(oil_prices))))
                    prod_vol = float(np.std(np.diff(np.log(opec_vals[:len(oil_prices)]))))
                    lerner_proxy = price_vol / prod_vol if prod_vol > 0 else 0
                else:
                    lerner_proxy = None

                results["opec_market_power"] = {
                    "market_share": round(latest_share, 3),
                    "market_share_trend": [round(float(s), 3) for s in market_share[-12:]],
                    "lerner_proxy": round(lerner_proxy, 3) if lerner_proxy is not None else None,
                    "n_obs": len(common_opec),
                }

        # --- Score ---
        score = 25.0  # baseline

        # Oil price volatility
        if "oil_price" in data:
            prices = np.array(list(data["oil_price"].values()))
            if len(prices) > 12:
                returns = np.diff(np.log(prices))
                vol = float(np.std(returns[-12:])) * np.sqrt(12)
                score += min(vol * 40, 25)

        # SPR adequacy
        spr_info = results.get("spr", {})
        if spr_info:
            days = spr_info.get("days_of_import_cover", 90)
            if days < 60:
                score += 20
            elif days < 90:
                score += 10

        # Pass-through magnitude
        pt_info = results.get("oil_cpi_passthrough", {})
        if pt_info:
            lr_pt = abs(pt_info.get("long_run", 0))
            score += min(lr_pt * 30, 15)

        # OPEC concentration
        opec_info = results.get("opec_market_power", {})
        if opec_info:
            share = opec_info.get("market_share", 0)
            if share > 0.40:
                score += 10

        score = float(np.clip(score, 0, 100))

        return {"score": round(score, 1), "results": results}
