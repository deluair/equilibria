"""Purchasing Power Parity (PPP) analysis module.

Methodology
-----------
Three PPP concepts tested:

1. **Absolute PPP**:
   e = P / P*
   The nominal exchange rate equals the ratio of domestic to foreign price
   levels. Rarely holds due to transportation costs, tariffs, non-traded goods.
   Tested via cross-sectional regression of log(e) on log(P/P*).

2. **Relative PPP**:
   delta_e = pi - pi*
   The rate of depreciation equals the inflation differential.
   Tested via time-series regression:
   delta_e_t = alpha + beta * (pi_t - pi*_t) + eps_t
   Under relative PPP: alpha = 0, beta = 1.

3. **Real exchange rate stationarity**:
   q_t = e_t + p*_t - p_t (all in logs)
   If PPP holds in the long run, q_t is stationary (mean-reverting).
   Tested via ADF and KPSS unit root tests.

**Half-life of PPP deviations**:
   From AR(1) on real exchange rate: q_t = rho * q_{t-1} + eps_t
   Half-life = -ln(2) / ln(rho)
   Literature consensus: 3-5 years (Rogoff 1996 "PPP puzzle").

**Penn effect** (Balassa-Samuelson):
   Countries with higher GDP per capita tend to have higher price levels.
   Cross-country regression: log(price_level) = alpha + beta * log(gdp_pc) + eps
   beta typically around 0.3-0.5.

Score reflects PPP deviation magnitude and persistence.

Sources: FRED, IMF IFS, Penn World Table, WDI
"""

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


def _adf_test(y: np.ndarray, max_lags: int = 12) -> dict:
    """Augmented Dickey-Fuller unit root test.

    Tests H0: unit root (non-stationary) vs H1: stationary.
    Includes constant. Lag order selected by BIC.
    """
    n = len(y)
    dy = np.diff(y)

    best_bic = np.inf
    best_p = 0

    for p in range(0, min(max_lags + 1, n // 4)):
        effective_n = len(dy) - p
        if effective_n < 5:
            continue

        Y = dy[p:]
        X_parts = [np.ones((effective_n, 1)), y[p : p + effective_n].reshape(-1, 1)]
        for lag in range(1, p + 1):
            X_parts.append(dy[p - lag : p - lag + effective_n].reshape(-1, 1))
        X = np.hstack(X_parts)

        beta = np.linalg.lstsq(X, Y, rcond=None)[0]
        resid = Y - X @ beta
        sigma2 = float(np.sum(resid ** 2)) / effective_n
        bic = effective_n * np.log(sigma2 + 1e-20) + X.shape[1] * np.log(effective_n)
        if bic < best_bic:
            best_bic = bic
            best_p = p

    # Fit with selected lag order
    p = best_p
    effective_n = len(dy) - p
    Y = dy[p:]
    X_parts = [np.ones((effective_n, 1)), y[p : p + effective_n].reshape(-1, 1)]
    for lag in range(1, p + 1):
        X_parts.append(dy[p - lag : p - lag + effective_n].reshape(-1, 1))
    X = np.hstack(X_parts)

    beta = np.linalg.lstsq(X, Y, rcond=None)[0]
    resid = Y - X @ beta
    sigma2 = float(np.sum(resid ** 2)) / (effective_n - X.shape[1])

    # ADF statistic = t-stat on the level coefficient (index 1)
    XtX_inv = np.linalg.inv(X.T @ X)
    se_gamma = float(np.sqrt(sigma2 * XtX_inv[1, 1]))
    adf_stat = float(beta[1]) / se_gamma if se_gamma > 0 else 0

    # Critical values (MacKinnon, approximate for constant-only model)
    # n=100: 1%=-3.51, 5%=-2.89, 10%=-2.58
    # n=250: 1%=-3.46, 5%=-2.87, 10%=-2.57
    # n=500: 1%=-3.44, 5%=-2.86, 10%=-2.57
    cv = {"1%": -3.48, "5%": -2.88, "10%": -2.58}

    return {
        "adf_statistic": round(adf_stat, 4),
        "lags": p,
        "critical_values": cv,
        "rejects_unit_root_5pct": adf_stat < cv["5%"],
        "rejects_unit_root_10pct": adf_stat < cv["10%"],
    }


def _kpss_test(y: np.ndarray, max_lags: int = None) -> dict:
    """KPSS stationarity test.

    Tests H0: stationary vs H1: unit root.
    Level stationarity (constant only).
    """
    n = len(y)
    if max_lags is None:
        max_lags = int(np.ceil(12 * (n / 100) ** 0.25))

    # Residuals from regression on constant
    y_mean = np.mean(y)
    resid = y - y_mean

    # Partial sum process
    S = np.cumsum(resid)

    # Long-run variance (Newey-West)
    gamma_0 = float(np.sum(resid ** 2)) / n
    lrv = gamma_0
    for lag in range(1, max_lags + 1):
        weight = 1 - lag / (max_lags + 1)  # Bartlett kernel
        gamma_j = float(np.sum(resid[lag:] * resid[:-lag])) / n
        lrv += 2 * weight * gamma_j

    # KPSS statistic
    kpss_stat = float(np.sum(S ** 2)) / (n ** 2 * lrv) if lrv > 0 else 0

    # Critical values (level stationarity)
    cv = {"10%": 0.347, "5%": 0.463, "2.5%": 0.574, "1%": 0.739}

    return {
        "kpss_statistic": round(kpss_stat, 4),
        "bandwidth": max_lags,
        "critical_values": cv,
        "rejects_stationarity_5pct": kpss_stat > cv["5%"],
    }


class PurchasingPowerParity(LayerBase):
    layer_id = "l2"
    name = "Purchasing Power Parity"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        base_country = kwargs.get("base_country", "USA")

        # For bilateral PPP, need a partner
        partner = kwargs.get("partner")
        if country == base_country and not partner:
            partner = "EUR"  # default bilateral partner

        results = {"country": country}

        # --- Relative PPP test (time-series) ---
        er_code = f"NEER_{country}" if not partner else f"EXRATE_{country}_{partner}"
        inf_code = f"INFLATION_{country}"
        inf_partner_code = f"INFLATION_{partner}" if partner else f"INFLATION_{base_country}"

        er_rows = await db.execute_fetchall(
            "SELECT date, value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE code = ?) ORDER BY date",
            (er_code,),
        )
        inf_rows = await db.execute_fetchall(
            "SELECT date, value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE code = ?) ORDER BY date",
            (inf_code,),
        )
        inf_p_rows = await db.execute_fetchall(
            "SELECT date, value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE code = ?) ORDER BY date",
            (inf_partner_code,),
        )

        if er_rows and inf_rows and inf_p_rows:
            er_dict = {r[0]: float(r[1]) for r in er_rows}
            inf_dict = {r[0]: float(r[1]) for r in inf_rows}
            inf_p_dict = {r[0]: float(r[1]) for r in inf_p_rows}
            common = sorted(set(er_dict) & set(inf_dict) & set(inf_p_dict))

            if len(common) >= 15:
                e_vals = np.array([np.log(er_dict[d]) for d in common])
                inf_vals = np.array([inf_dict[d] for d in common])
                inf_p_vals = np.array([inf_p_dict[d] for d in common])

                # delta_e vs (pi - pi*)
                de = np.diff(e_vals) * 100  # percent
                inf_diff = inf_vals[1:] - inf_p_vals[1:]

                n_rel = len(de)
                X_rel = np.column_stack([np.ones(n_rel), inf_diff])
                beta_rel = np.linalg.lstsq(X_rel, de, rcond=None)[0]
                resid_rel = de - X_rel @ beta_rel
                sse_rel = float(np.sum(resid_rel ** 2))
                sst_rel = float(np.sum((de - np.mean(de)) ** 2))
                r2_rel = 1 - sse_rel / sst_rel if sst_rel > 0 else 0.0

                # SE
                XtX_inv = np.linalg.inv(X_rel.T @ X_rel)
                sigma2 = sse_rel / (n_rel - 2)
                se_rel = np.sqrt(sigma2 * np.diag(XtX_inv))

                # Test beta = 1 (relative PPP)
                t_beta1 = (beta_rel[1] - 1) / se_rel[1] if se_rel[1] > 0 else 0
                p_beta1 = 2 * (1 - stats.t.cdf(abs(t_beta1), n_rel - 2))

                results["relative_ppp"] = {
                    "n_obs": n_rel,
                    "intercept": round(float(beta_rel[0]), 4),
                    "slope": round(float(beta_rel[1]), 4),
                    "slope_se": round(float(se_rel[1]), 4),
                    "r_squared": round(r2_rel, 4),
                    "test_slope_equals_1": {
                        "t_stat": round(float(t_beta1), 3),
                        "p_value": round(float(p_beta1), 4),
                        "rejects": float(p_beta1) < 0.05,
                    },
                    "holds": float(p_beta1) >= 0.05 and abs(beta_rel[1] - 1) < 0.5,
                }

                # --- Real exchange rate and stationarity ---
                # q_t = e_t + p*_t - p_t (in log levels)
                # Approximate: construct from cumulated inflation differentials
                # q_t = e_t - cumsum(pi_t - pi*_t) (up to a constant)
                cum_inf_diff = np.cumsum(inf_vals - inf_p_vals) / 100
                q = e_vals - cum_inf_diff  # real exchange rate (log)

                # ADF test on real exchange rate
                adf_result = _adf_test(q)
                kpss_result = _kpss_test(q)

                results["real_exchange_rate"] = {
                    "series": q.tolist(),
                    "dates": common,
                    "mean": float(np.mean(q)),
                    "std": float(np.std(q, ddof=1)),
                    "adf_test": adf_result,
                    "kpss_test": kpss_result,
                    "stationary": adf_result["rejects_unit_root_5pct"],
                    "interpretation": (
                        "Real exchange rate is stationary (PPP holds long-run)"
                        if adf_result["rejects_unit_root_5pct"]
                        else "Cannot reject unit root in real exchange rate (PPP may not hold)"
                    ),
                }

                # --- Half-life of PPP deviations ---
                q_demeaned = q - np.mean(q)
                if len(q_demeaned) > 5:
                    # AR(1): q_t = rho * q_{t-1} + eps
                    q_lag = q_demeaned[:-1]
                    q_curr = q_demeaned[1:]
                    rho = float(np.dot(q_lag, q_curr) / np.dot(q_lag, q_lag))

                    if 0 < rho < 1:
                        half_life = -np.log(2) / np.log(rho)

                        # Confidence interval via delta method
                        n_hl = len(q_curr)
                        resid_hl = q_curr - rho * q_lag
                        sigma2_hl = float(np.sum(resid_hl ** 2)) / (n_hl - 1)
                        var_rho = sigma2_hl / float(np.sum(q_lag ** 2))
                        se_rho = float(np.sqrt(var_rho))

                        # Delta method for half-life SE
                        d_hl_d_rho = np.log(2) / (rho * (np.log(rho)) ** 2)
                        se_hl = abs(d_hl_d_rho) * se_rho

                        results["half_life"] = {
                            "rho": round(rho, 4),
                            "rho_se": round(se_rho, 4),
                            "half_life_periods": round(float(half_life), 1),
                            "half_life_years": round(float(half_life / 4), 1),  # quarterly
                            "se_periods": round(float(se_hl), 1),
                            "ci_95_years": [
                                round(float((half_life - 1.96 * se_hl) / 4), 1),
                                round(float((half_life + 1.96 * se_hl) / 4), 1),
                            ],
                            "consistent_with_literature": 2 < half_life / 4 < 6,
                        }
                    else:
                        results["half_life"] = {
                            "rho": round(rho, 4),
                            "note": "rho outside (0,1), half-life undefined or infinite",
                        }

        # --- Penn effect (cross-country) ---
        # Fetch cross-country price level and GDP per capita data
        price_level_rows = await db.execute_fetchall(
            "SELECT date, value FROM data_points dp "
            "JOIN data_series ds ON dp.series_id = ds.id "
            "WHERE ds.code LIKE 'PRICE_LEVEL_%' ORDER BY ds.code, dp.date",
        )
        gdp_pc_rows = await db.execute_fetchall(
            "SELECT date, value FROM data_points dp "
            "JOIN data_series ds ON dp.series_id = ds.id "
            "WHERE ds.code LIKE 'GDP_PC_PPP_%' ORDER BY ds.code, dp.date",
        )

        if price_level_rows and gdp_pc_rows:
            # Use latest observation per country
            pl_latest = {}
            for r in price_level_rows:
                # Extract country from code suffix
                code = r[0] if isinstance(r[0], str) else ""
                val = float(r[1])
                if code:
                    pl_latest[code] = val

            gdp_latest = {}
            for r in gdp_pc_rows:
                code = r[0] if isinstance(r[0], str) else ""
                val = float(r[1])
                if code:
                    gdp_latest[code] = val

            common_countries = set(pl_latest) & set(gdp_latest)
            if len(common_countries) >= 10:
                pl_arr = np.array([np.log(pl_latest[c]) for c in common_countries])
                gdp_arr = np.array([np.log(gdp_latest[c]) for c in common_countries])

                n_penn = len(pl_arr)
                X_penn = np.column_stack([np.ones(n_penn), gdp_arr])
                beta_penn = np.linalg.lstsq(X_penn, pl_arr, rcond=None)[0]
                resid_penn = pl_arr - X_penn @ beta_penn
                sse_penn = float(np.sum(resid_penn ** 2))
                sst_penn = float(np.sum((pl_arr - np.mean(pl_arr)) ** 2))
                r2_penn = 1 - sse_penn / sst_penn if sst_penn > 0 else 0.0

                results["penn_effect"] = {
                    "n_countries": n_penn,
                    "slope": round(float(beta_penn[1]), 4),
                    "r_squared": round(r2_penn, 4),
                    "interpretation": (
                        f"A 10% higher GDP per capita is associated with "
                        f"{beta_penn[1]*10:.1f}% higher price level"
                    ),
                }

        # --- Score ---
        # Large real exchange rate deviations -> misalignment risk
        rer_penalty = 0
        if "real_exchange_rate" in results:
            rer_std = results["real_exchange_rate"]["std"]
            rer_penalty = min(rer_std * 20, 25)

        # Non-stationary real exchange rate (PPP doesn't hold)
        unit_root_penalty = 0
        if "real_exchange_rate" in results and not results["real_exchange_rate"]["stationary"]:
            unit_root_penalty = 20

        # Very long half-life
        hl_penalty = 0
        if "half_life" in results and "half_life_years" in results["half_life"]:
            hl_years = results["half_life"]["half_life_years"]
            if hl_years > 5:
                hl_penalty = 15
            elif hl_years > 3:
                hl_penalty = 5

        # Relative PPP failure
        rel_penalty = 0
        if "relative_ppp" in results and not results["relative_ppp"]["holds"]:
            rel_penalty = 15

        score = min(rer_penalty + unit_root_penalty + hl_penalty + rel_penalty, 100)

        results["n_obs"] = results.get("relative_ppp", {}).get("n_obs", 0)

        return {"score": round(score, 1), "results": results}
