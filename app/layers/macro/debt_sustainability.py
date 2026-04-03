"""Debt Sustainability analysis module.

Methodology
-----------
Core framework: the government budget constraint and debt dynamics equation.

**Debt dynamics equation**:
    d_t = d_{t-1} * (1 + r_t) / (1 + g_t) - pb_t

where:
    d_t  = debt-to-GDP ratio at time t
    r_t  = effective real interest rate on government debt
    g_t  = real GDP growth rate
    pb_t = primary balance as share of GDP (positive = surplus)

**The r-g differential** is the key sustainability indicator:
    - If r < g: debt-to-GDP ratio declines even without a primary surplus
      (debt is dynamically efficient but stable)
    - If r > g: a primary surplus is required to stabilize debt

**Required primary balance** to stabilize debt at current level:
    pb* = d * (r - g) / (1 + g)

**Debt trajectory simulation**:
    Projects debt-to-GDP under baseline, optimistic, and pessimistic scenarios
    using stochastic simulation of r, g, and pb paths (Monte Carlo with
    historical distributions).

**Sustainability indicators**:
    - Blanchard (1990) tax gap: required vs actual tax rate to stabilize debt
    - Bohn (1998) fiscal reaction function: pb_t = rho * d_{t-1} + controls
      If rho > 0, fiscal policy is sustainable (primary balance responds
      positively to debt accumulation)

Score reflects debt risk: high debt, adverse r-g, weak fiscal reaction.

Sources: FRED, IMF WEO/IFS, WDI
"""

import numpy as np

from app.layers.base import LayerBase


class DebtSustainability(LayerBase):
    layer_id = "l2"
    name = "Debt Sustainability"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        projection_years = kwargs.get("projection_years", 10)
        n_simulations = kwargs.get("n_simulations", 1000)

        # Fetch data
        series_map = {
            "debt_gdp": f"DEBT_GDP_{country}",
            "primary_balance_gdp": f"PRIMARY_BAL_GDP_{country}",
            "real_interest": f"REAL_INTEREST_{country}",
            "real_growth": f"REAL_GROWTH_{country}",
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

        if not data.get("debt_gdp"):
            return {"score": 50, "results": {"error": "no debt-to-GDP data"}}

        debt = data["debt_gdp"]["values"]
        dates = data["debt_gdp"]["dates"]

        results = {
            "country": country,
            "n_obs": len(debt),
            "period": f"{dates[0]} to {dates[-1]}",
        }

        # Current debt level
        d_latest = float(debt[-1])
        results["debt_gdp_latest"] = d_latest

        # --- r-g analysis ---
        r_vals = data.get("real_interest", {}).get("values")
        g_vals = data.get("real_growth", {}).get("values")

        if r_vals is not None and g_vals is not None:
            # Align to common length
            min_len = min(len(r_vals), len(g_vals), len(debt))
            r = r_vals[-min_len:] / 100  # convert from percent
            g = g_vals[-min_len:] / 100
            d = debt[-min_len:]
            r_g = r - g

            results["r_minus_g"] = {
                "latest": float(r_g[-1]) * 100,
                "mean": float(np.mean(r_g)) * 100,
                "std": float(np.std(r_g, ddof=1)) * 100 if len(r_g) > 1 else 0.0,
                "pct_negative": float(np.mean(r_g < 0)) * 100,
                "series": (r_g * 100).tolist(),
                "favorable": bool(r_g[-1] < 0),
            }

            # Required primary balance to stabilize debt
            r_latest = float(r[-1])
            g_latest = float(g[-1])
            d_ratio = d_latest / 100  # convert from percent of GDP
            if abs(1 + g_latest) > 0.001:
                pb_required = d_ratio * (r_latest - g_latest) / (1 + g_latest) * 100
            else:
                pb_required = 0.0

            results["required_primary_balance"] = {
                "stabilizing_pct_gdp": round(pb_required, 2),
                "interpretation": (
                    f"A primary {'surplus' if pb_required > 0 else 'deficit'} of "
                    f"{abs(pb_required):.2f}% of GDP is needed to stabilize debt"
                ),
            }

            # --- Bohn fiscal reaction function ---
            pb_vals = data.get("primary_balance_gdp", {}).get("values")
            if pb_vals is not None:
                pb_len = min(len(pb_vals), len(debt))
                pb = pb_vals[-pb_len:]
                d_lag = debt[-pb_len:][:-1]
                pb_curr = pb[1:]

                n_bohn = len(pb_curr)
                if n_bohn >= 10:
                    X_bohn = np.column_stack([np.ones(n_bohn), d_lag])
                    beta_bohn = np.linalg.lstsq(X_bohn, pb_curr, rcond=None)[0]
                    resid_bohn = pb_curr - X_bohn @ beta_bohn
                    sse_bohn = float(np.sum(resid_bohn ** 2))
                    sst_bohn = float(np.sum((pb_curr - np.mean(pb_curr)) ** 2))
                    r2_bohn = 1 - sse_bohn / sst_bohn if sst_bohn > 0 else 0.0

                    # HC1 SE
                    bread = np.linalg.inv(X_bohn.T @ X_bohn)
                    meat = X_bohn.T @ np.diag(resid_bohn ** 2) @ X_bohn
                    vcov = (n_bohn / (n_bohn - 2)) * bread @ meat @ bread
                    se_bohn = np.sqrt(np.diag(vcov))

                    rho = float(beta_bohn[1])
                    results["bohn_reaction"] = {
                        "rho": round(rho, 4),
                        "rho_se": round(float(se_bohn[1]), 4),
                        "rho_t_stat": round(rho / float(se_bohn[1]), 2) if se_bohn[1] > 0 else 0,
                        "r_squared": round(r2_bohn, 4),
                        "sustainable": rho > 0,
                        "interpretation": (
                            "Fiscal policy responds "
                            + ("positively" if rho > 0 else "negatively")
                            + f" to debt (rho={rho:.4f}). "
                            + ("Consistent" if rho > 0 else "Inconsistent")
                            + " with long-run sustainability."
                        ),
                    }

            # --- Debt trajectory simulation ---
            # Historical distributions
            r_mean = float(np.mean(r))
            r_std = float(np.std(r, ddof=1)) if len(r) > 1 else 0.01
            g_mean = float(np.mean(g))
            g_std = float(np.std(g, ddof=1)) if len(g) > 1 else 0.01

            pb_mean = 0.0
            pb_std = 0.01
            if pb_vals is not None and len(pb_vals) > 1:
                pb_mean = float(np.mean(pb_vals)) / 100
                pb_std = float(np.std(pb_vals, ddof=1)) / 100

            rng = np.random.default_rng(42)
            trajectories = np.zeros((n_simulations, projection_years + 1))
            trajectories[:, 0] = d_latest

            for t in range(projection_years):
                r_sim = rng.normal(r_mean, r_std, n_simulations)
                g_sim = rng.normal(g_mean, g_std, n_simulations)
                pb_sim = rng.normal(pb_mean, pb_std, n_simulations)

                for s in range(n_simulations):
                    d_prev = trajectories[s, t]
                    denom = 1 + g_sim[s]
                    if abs(denom) > 0.001:
                        trajectories[s, t + 1] = d_prev * (1 + r_sim[s]) / denom - pb_sim[s] * 100
                    else:
                        trajectories[s, t + 1] = d_prev

            # Percentiles
            pctiles = [10, 25, 50, 75, 90]
            proj_percentiles = {}
            for p in pctiles:
                proj_percentiles[f"p{p}"] = [round(float(v), 1) for v in np.percentile(trajectories, p, axis=0)]

            results["projection"] = {
                "years": projection_years,
                "n_simulations": n_simulations,
                "percentiles": proj_percentiles,
                "prob_above_100": round(float(np.mean(trajectories[:, -1] > 100)) * 100, 1),
                "prob_above_150": round(float(np.mean(trajectories[:, -1] > 150)) * 100, 1),
                "terminal_median": round(float(np.median(trajectories[:, -1])), 1),
            }

            # --- Blanchard tax gap ---
            # Simplified: difference between required and actual primary balance
            actual_pb = float(pb_vals[-1]) if pb_vals is not None and len(pb_vals) > 0 else 0.0
            tax_gap = pb_required - actual_pb
            results["blanchard_tax_gap"] = {
                "gap_pct_gdp": round(tax_gap, 2),
                "requires_adjustment": tax_gap > 0,
            }

        # --- Historical context ---
        results["historical"] = {
            "debt_series": debt.tolist(),
            "dates": dates,
            "peak": {"value": float(np.max(debt)), "date": dates[int(np.argmax(debt))]},
            "trough": {"value": float(np.min(debt)), "date": dates[int(np.argmin(debt))]},
            "change_last_5y": float(debt[-1] - debt[-5]) if len(debt) >= 5 else None,
            "change_last_10y": float(debt[-1] - debt[-10]) if len(debt) >= 10 else None,
        }

        # --- Score ---
        # High debt level
        if d_latest > 120:
            debt_penalty = 40
        elif d_latest > 90:
            debt_penalty = 25
        elif d_latest > 60:
            debt_penalty = 15
        else:
            debt_penalty = 5

        # Adverse r-g
        rg_penalty = 0
        if "r_minus_g" in results:
            rg_latest = results["r_minus_g"]["latest"]
            if rg_latest > 2:
                rg_penalty = 25
            elif rg_latest > 0:
                rg_penalty = 10

        # Weak fiscal reaction
        bohn_penalty = 0
        if "bohn_reaction" in results and not results["bohn_reaction"]["sustainable"]:
            bohn_penalty = 20

        # Explosive trajectory
        proj_penalty = 0
        if "projection" in results:
            if results["projection"]["prob_above_150"] > 50:
                proj_penalty = 15
            elif results["projection"]["prob_above_100"] > 75:
                proj_penalty = 10

        score = min(debt_penalty + rg_penalty + bohn_penalty + proj_penalty, 100)

        return {"score": round(score, 1), "results": results}
