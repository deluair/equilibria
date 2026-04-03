"""Fiscal Space - Ostry et al. (2010) fiscal space estimation.

Methodology
-----------
Fiscal space = distance between current debt and the debt limit
implied by historical fiscal behavior.

**Debt limit** (Ostry, Ghosh, Kim & Qureshi, 2010):
    The maximum debt level at which the government can still stabilize
    debt through primary balance adjustments. Derived from the fiscal
    reaction function (Bohn 1998):

        pb_t = f(d_{t-1}) + controls + e_t

    where pb = primary balance/GDP, d = debt/GDP.

    The reaction function is typically cubic:
        pb_t = a + b1*d_{t-1} + b2*d_{t-1}^2 + b3*d_{t-1}^3 + controls

    Fiscal fatigue: at very high debt levels, the primary balance response
    weakens (b3 < 0), implying a debt limit.

**Debt limit computation**:
    d_limit solves: f(d) = (r-g)/(1+g) * d
    The intersection of the fiscal reaction function with the debt
    stabilizing line. Fiscal space = d_limit - d_current.

**Stochastic simulation**:
    Account for uncertainty in r, g, and primary balance to compute
    a probability distribution of fiscal space.

**Primary balance sustainability gap**:
    Difference between the primary balance needed to stabilize debt
    and the historically achievable primary balance.

References:
- Ostry, Ghosh, Kim & Qureshi (2010), "Fiscal Space," IMF SPN/10/11
- Ghosh, Kim, Mendoza, Ostry & Qureshi (2013), "Fiscal Fatigue, Fiscal Space,
  and Debt Sustainability in Advanced Economies," EJ
- Bohn (1998), "The Behavior of U.S. Public Debt and Deficits," QJE
"""

from __future__ import annotations

import numpy as np
from scipy import optimize as sp_optimize

from app.layers.base import LayerBase


class FiscalSpace(LayerBase):
    layer_id = "l2"
    name = "Fiscal Space"
    weight = 0.05

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        n_simulations = kwargs.get("n_simulations", 2000)
        projection_years = kwargs.get("projection_years", 10)

        # Fetch data
        series_map = {
            "debt_gdp": f"DEBT_GDP_{country}",
            "primary_balance": f"PRIMARY_BAL_GDP_{country}",
            "real_interest": f"REAL_INTEREST_{country}",
            "real_growth": f"REAL_GROWTH_{country}",
            "revenue_gdp": f"REVENUE_GDP_{country}",
            "expenditure_gdp": f"EXPENDITURE_GDP_{country}",
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

        if "debt_gdp" not in data:
            return {"score": 50.0, "results": {"error": "no debt-to-GDP data"}}

        debt = data["debt_gdp"]["values"]
        dates = data["debt_gdp"]["dates"]
        T = len(debt)
        d_current = float(debt[-1])

        results = {
            "country": country,
            "n_obs": T,
            "period": f"{dates[0]} to {dates[-1]}",
            "current_debt_gdp": round(d_current, 1),
        }

        # --- Fiscal Reaction Function (Bohn) ---
        pb_data = data.get("primary_balance", {}).get("values")
        if pb_data is not None and len(pb_data) >= 15:
            frf = self._estimate_fiscal_reaction(debt, pb_data)
            results["fiscal_reaction"] = frf
        else:
            frf = None
            results["fiscal_reaction"] = {"note": "insufficient primary balance data"}

        # --- Debt limit estimation ---
        r_data = data.get("real_interest", {}).get("values")
        g_data = data.get("real_growth", {}).get("values")

        if r_data is not None and g_data is not None and frf is not None:
            r_mean = float(np.mean(r_data[-20:])) / 100 if len(r_data) >= 20 else float(np.mean(r_data)) / 100
            g_mean = float(np.mean(g_data[-20:])) / 100 if len(g_data) >= 20 else float(np.mean(g_data)) / 100

            debt_limit = self._compute_debt_limit(frf, r_mean, g_mean)
            results["debt_limit"] = debt_limit

            # Fiscal space
            if debt_limit.get("limit") is not None:
                fiscal_space = debt_limit["limit"] - d_current
                results["fiscal_space"] = {
                    "space_pct_gdp": round(fiscal_space, 1),
                    "ample": fiscal_space > 50,
                    "moderate": 20 < fiscal_space <= 50,
                    "narrow": 0 < fiscal_space <= 20,
                    "exhausted": fiscal_space <= 0,
                    "category": (
                        "ample" if fiscal_space > 50
                        else "moderate" if fiscal_space > 20
                        else "narrow" if fiscal_space > 0
                        else "exhausted"
                    ),
                }
            else:
                results["fiscal_space"] = {
                    "note": "debt limit could not be computed (no fiscal fatigue detected)"
                }

            # --- Stochastic fiscal space ---
            stochastic = self._stochastic_fiscal_space(
                d_current, pb_data, r_data, g_data,
                frf, n_simulations, projection_years
            )
            results["stochastic"] = stochastic

        # --- Primary balance sustainability gap ---
        if pb_data is not None and r_data is not None and g_data is not None:
            gap = self._sustainability_gap(d_current, pb_data, r_data, g_data)
            results["sustainability_gap"] = gap

        # --- Fiscal fatigue analysis ---
        if frf is not None:
            fatigue = self._fiscal_fatigue_analysis(frf, d_current)
            results["fiscal_fatigue"] = fatigue

        # --- Historical primary balance capacity ---
        if pb_data is not None:
            results["primary_balance_history"] = {
                "latest": round(float(pb_data[-1]), 2),
                "mean": round(float(np.mean(pb_data)), 2),
                "max_surplus": round(float(np.max(pb_data)), 2),
                "max_deficit": round(float(np.min(pb_data)), 2),
                "std": round(float(np.std(pb_data, ddof=1)), 2) if len(pb_data) > 1 else 0.0,
                "pct_surplus": round(float(np.mean(pb_data > 0)) * 100, 1),
            }

        # --- Score ---
        score = self._compute_score(results)

        return {"score": round(score, 1), "results": results}

    @staticmethod
    def _estimate_fiscal_reaction(debt: np.ndarray, pb: np.ndarray) -> dict:
        """Estimate cubic fiscal reaction function:
        pb_t = a + b1*d_{t-1} + b2*d_{t-1}^2 + b3*d_{t-1}^3 + e
        """
        min_len = min(len(debt), len(pb))
        d_lag = debt[-min_len:][:-1]
        pb_curr = pb[-min_len:][1:]
        n = len(pb_curr)

        if n < 15:
            return {"note": "too few observations"}

        # Normalize debt for numerical stability
        d_mean = float(np.mean(d_lag))
        d_std = max(float(np.std(d_lag, ddof=1)), 1.0)
        d_norm = (d_lag - d_mean) / d_std

        # Cubic specification
        X = np.column_stack([
            np.ones(n),
            d_norm,
            d_norm ** 2,
            d_norm ** 3,
        ])

        beta = np.linalg.lstsq(X, pb_curr, rcond=None)[0]
        resid = pb_curr - X @ beta
        sst = float(np.sum((pb_curr - np.mean(pb_curr)) ** 2))
        sse = float(np.sum(resid ** 2))
        r2 = 1 - sse / sst if sst > 0 else 0.0

        # Standard errors (HC1)
        k = X.shape[1]
        try:
            bread = np.linalg.inv(X.T @ X)
            meat = X.T @ np.diag(resid ** 2) @ X
            vcov = (n / (n - k)) * bread @ meat @ bread
            se = np.sqrt(np.diag(vcov))
        except np.linalg.LinAlgError:
            se = np.zeros(k)

        # Bohn coefficient (linear response)
        bohn_rho = float(beta[1]) / d_std  # rescale to original units

        # Convert coefficients back to original scale for interpretation
        # Original: pb = a + b1*(d-dmean)/dstd + b2*((d-dmean)/dstd)^2 + b3*((d-dmean)/dstd)^3
        coefficients = {
            "constant": round(float(beta[0]), 4),
            "linear": round(float(beta[1]), 4),
            "quadratic": round(float(beta[2]), 4),
            "cubic": round(float(beta[3]), 4),
        }

        se_dict = {
            "constant": round(float(se[0]), 4),
            "linear": round(float(se[1]), 4),
            "quadratic": round(float(se[2]), 4),
            "cubic": round(float(se[3]), 4),
        }

        return {
            "coefficients": coefficients,
            "standard_errors": se_dict,
            "r_squared": round(r2, 4),
            "bohn_rho": round(bohn_rho, 4),
            "bohn_sustainable": bohn_rho > 0,
            "normalization": {"mean": round(d_mean, 2), "std": round(d_std, 2)},
            "n_obs": n,
            "cubic_negative": float(beta[3]) < 0,
            "fiscal_fatigue_detected": float(beta[3]) < 0 and abs(float(beta[3]) / max(se[3], 1e-6)) > 1.5,
            "_beta": beta.tolist(),
        }

    @staticmethod
    def _compute_debt_limit(frf: dict, r_mean: float, g_mean: float) -> dict:
        """Find debt limit: where fiscal reaction function intersects
        the debt-stabilizing line."""
        beta = np.array(frf.get("_beta", [0, 0, 0, 0]))
        d_mean = frf["normalization"]["mean"]
        d_std = frf["normalization"]["std"]

        # Debt-stabilizing primary balance: pb* = d * (r - g) / (1 + g)
        rg_factor = (r_mean - g_mean) / max(1 + g_mean, 0.01)

        # Find where f(d) = rg_factor * d
        # f(d) = beta[0] + beta[1]*((d-dmean)/dstd) + beta[2]*((d-dmean)/dstd)^2 + beta[3]*((d-dmean)/dstd)^3
        def gap(d_pct):
            d_norm = (d_pct - d_mean) / d_std
            pb_response = beta[0] + beta[1] * d_norm + beta[2] * d_norm ** 2 + beta[3] * d_norm ** 3
            pb_required = rg_factor * d_pct
            return pb_response - pb_required

        # Search for upper crossing (debt limit) in range [50, 300]
        try:
            # Find where the reaction function drops below the stabilizing line
            d_test = np.linspace(30, 300, 1000)
            gaps = [gap(d) for d in d_test]

            # Look for sign changes from positive to negative
            limit = None
            for i in range(len(gaps) - 1):
                if gaps[i] > 0 and gaps[i + 1] <= 0 and d_test[i] > d_mean:
                    # Bisect
                    result = sp_optimize.brentq(gap, d_test[i], d_test[i + 1])
                    limit = float(result)
                    break

            if limit is not None:
                return {
                    "limit": round(limit, 1),
                    "r_minus_g": round((r_mean - g_mean) * 100, 3),
                    "method": "fiscal_reaction_intersection",
                }
            else:
                return {
                    "limit": None,
                    "note": "no debt limit found in [30, 300] range (fiscal fatigue may not be present)",
                    "r_minus_g": round((r_mean - g_mean) * 100, 3),
                }

        except Exception:
            return {"limit": None, "note": "numerical error in debt limit computation"}

    @staticmethod
    def _stochastic_fiscal_space(d_current: float, pb: np.ndarray,
                                  r: np.ndarray, g: np.ndarray,
                                  frf: dict | None, n_sim: int,
                                  horizon: int) -> dict:
        """Monte Carlo simulation of debt trajectories."""
        # Historical distributions
        min_len = min(len(r), len(g), len(pb))
        r_vals = r[-min_len:] / 100
        g_vals = g[-min_len:] / 100
        pb_vals = pb[-min_len:] / 100

        r_mean = float(np.mean(r_vals))
        g_mean = float(np.mean(g_vals))
        pb_mean = float(np.mean(pb_vals))

        r_std = max(float(np.std(r_vals, ddof=1)), 0.005) if len(r_vals) > 1 else 0.01
        g_std = max(float(np.std(g_vals, ddof=1)), 0.005) if len(g_vals) > 1 else 0.01
        pb_std = max(float(np.std(pb_vals, ddof=1)), 0.002) if len(pb_vals) > 1 else 0.005

        # Correlation between r and g
        if min_len > 5:
            rho_rg = float(np.corrcoef(r_vals, g_vals)[0, 1])
        else:
            rho_rg = 0.0

        rng = np.random.default_rng(42)
        trajectories = np.zeros((n_sim, horizon + 1))
        trajectories[:, 0] = d_current

        for t in range(horizon):
            # Correlated draws for r and g
            z1 = rng.standard_normal(n_sim)
            z2 = rng.standard_normal(n_sim)
            r_sim = r_mean + r_std * z1
            g_sim = g_mean + g_std * (rho_rg * z1 + np.sqrt(max(1 - rho_rg ** 2, 0)) * z2)
            pb_sim = rng.normal(pb_mean, pb_std, n_sim)

            for s in range(n_sim):
                d_prev = trajectories[s, t]
                denom = 1 + g_sim[s]
                if abs(denom) > 0.001:
                    trajectories[s, t + 1] = d_prev * (1 + r_sim[s]) / denom - pb_sim[s] * 100
                else:
                    trajectories[s, t + 1] = d_prev

        terminal = trajectories[:, -1]

        return {
            "n_simulations": n_sim,
            "horizon_years": horizon,
            "terminal_debt_median": round(float(np.median(terminal)), 1),
            "terminal_debt_p10": round(float(np.percentile(terminal, 10)), 1),
            "terminal_debt_p90": round(float(np.percentile(terminal, 90)), 1),
            "prob_above_100": round(float(np.mean(terminal > 100)) * 100, 1),
            "prob_above_150": round(float(np.mean(terminal > 150)) * 100, 1),
            "prob_decline": round(float(np.mean(terminal < d_current)) * 100, 1),
            "r_g_correlation": round(rho_rg, 3),
        }

    @staticmethod
    def _sustainability_gap(d_current: float, pb: np.ndarray,
                            r: np.ndarray, g: np.ndarray) -> dict:
        """Gap between required and achievable primary balance."""
        r_mean = float(np.mean(r[-20:])) / 100 if len(r) >= 20 else float(np.mean(r)) / 100
        g_mean = float(np.mean(g[-20:])) / 100 if len(g) >= 20 else float(np.mean(g)) / 100

        d_ratio = d_current / 100
        denom = max(1 + g_mean, 0.01)
        pb_required = d_ratio * (r_mean - g_mean) / denom * 100  # percent of GDP

        # Historical maximum sustained surplus (5-year average)
        if len(pb) >= 5:
            rolling_avg = np.convolve(pb, np.ones(5) / 5, mode="valid")
            max_sustained = float(np.max(rolling_avg))
        else:
            max_sustained = float(np.max(pb))

        gap = pb_required - max_sustained

        return {
            "required_primary_balance": round(pb_required, 2),
            "max_sustained_surplus": round(max_sustained, 2),
            "gap": round(gap, 2),
            "sustainable": gap <= 0,
            "interpretation": (
                f"Required primary balance: {pb_required:.2f}% of GDP. "
                f"Maximum historically sustained: {max_sustained:.2f}%. "
                + ("Achievable." if gap <= 0 else f"Gap of {gap:.2f} pp is concerning.")
            ),
        }

    @staticmethod
    def _fiscal_fatigue_analysis(frf: dict, d_current: float) -> dict:
        """Assess fiscal fatigue at current debt level."""
        beta = np.array(frf.get("_beta", [0, 0, 0, 0]))
        d_mean = frf["normalization"]["mean"]
        d_std = frf["normalization"]["std"]

        # Marginal response at current debt
        d_norm = (d_current - d_mean) / d_std
        marginal = (beta[1] + 2 * beta[2] * d_norm + 3 * beta[3] * d_norm ** 2) / d_std

        # Is the marginal response declining?
        # Check at d_current vs at mean
        marginal_at_mean = beta[1] / d_std
        fatigue_ratio = marginal / marginal_at_mean if abs(marginal_at_mean) > 1e-6 else 1.0

        return {
            "marginal_response_current": round(float(marginal), 4),
            "marginal_response_mean_debt": round(float(marginal_at_mean), 4),
            "fatigue_ratio": round(float(fatigue_ratio), 3),
            "fatigue_present": float(fatigue_ratio) < 0.5,
            "response_declining": float(marginal) < float(marginal_at_mean),
            "interpretation": (
                "Fiscal fatigue detected: primary balance response weakening at current debt"
                if float(fatigue_ratio) < 0.5
                else "Fiscal response remains adequate at current debt level"
            ),
        }

    @staticmethod
    def _compute_score(results: dict) -> float:
        """Score based on fiscal space assessment."""
        score = 10.0

        # Current debt level
        d = results.get("current_debt_gdp", 0)
        if d > 120:
            score += 30
        elif d > 90:
            score += 20
        elif d > 60:
            score += 10

        # Fiscal space category
        fs = results.get("fiscal_space", {})
        category = fs.get("category")
        if category == "exhausted":
            score += 30
        elif category == "narrow":
            score += 20
        elif category == "moderate":
            score += 10

        # Sustainability gap
        sg = results.get("sustainability_gap", {})
        if not sg.get("sustainable", True):
            gap = abs(sg.get("gap", 0))
            score += min(gap * 5, 20)

        # Fiscal fatigue
        ff = results.get("fiscal_fatigue", {})
        if ff.get("fatigue_present", False):
            score += 10

        # Weak Bohn response
        frf = results.get("fiscal_reaction", {})
        if not frf.get("bohn_sustainable", True):
            score += 10

        return min(score, 100)
