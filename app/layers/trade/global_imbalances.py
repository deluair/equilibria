"""Global Imbalances analysis module.

Methodology
-----------
Global imbalances refer to large and persistent current account surpluses and
deficits across countries, a key source of systemic financial risk
(Obstfeld and Rogoff 2009).

Analysis dimensions:

1. Current account persistence: AR(1) coefficient on CA/GDP ratio.
   High persistence (rho > 0.7) signals structural imbalance.

2. Chinn-Prasad savings-investment regression (2003):
   CA/GDP_it = alpha + beta_1 * (S-I)_it + beta_2 * fiscal_balance_it
               + beta_3 * NFA_it + beta_4 * financial_develop_it + e_it
   Decomposes imbalance into savings glut vs investment drought.

3. Reserve accumulation: change in FX reserves as % of GDP.
   Large accumulation by surplus countries reflects mercantilist motives
   (Dooley, Folkerts-Landau & Garber 2003 -- "Bretton Woods II").

4. Twin surpluses/deficits: simultaneous fiscal and current account
   surpluses (China 2000s) or deficits (US 2000s).

Score (0-100): higher score indicates larger, more persistent imbalance
with associated global spillover risk.

References:
    Chinn, M. and Prasad, E. (2003). "Medium-term determinants of current
        accounts in industrial and developing countries." Journal of
        International Economics, 59(1), 47-76.
    Obstfeld, M. and Rogoff, K. (2009). "Global Imbalances and the
        Financial Crisis: Products of Common Causes." Federal Reserve Bank
        of San Francisco Asia Economic Policy Conference.
    Dooley, M.P., Folkerts-Landau, D. and Garber, P. (2003). "An Essay
        on the Revived Bretton Woods System." NBER Working Paper 9971.
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class GlobalImbalances(LayerBase):
    layer_id = "l1"
    name = "Global Imbalances"

    async def compute(self, db, **kwargs) -> dict:
        """Compute global imbalances indicators.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country : str - ISO3 country code
            year    : int - reference year
        """
        country = kwargs.get("country", "USA")
        year = kwargs.get("year")

        series_codes = {
            "current_account": f"CURR_ACCT_GDP_{country}",
            "fiscal_balance":  f"FISCAL_BAL_GDP_{country}",
            "gross_savings":   f"GROSS_SAV_GDP_{country}",
            "gross_invest":    f"GROSS_INV_GDP_{country}",
            "reserves":        f"FX_RESERVES_GDP_{country}",
            "nfa":             f"NET_FOREIGN_ASSETS_{country}",
            "fin_develop":     f"PRIV_CREDIT_GDP_{country}",
        }

        data: dict[str, np.ndarray] = {}
        dates_map: dict[str, list] = {}

        for label, code in series_codes.items():
            rows = await db.execute_fetchall(
                "SELECT date, value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE code = ?) ORDER BY date",
                (code,),
            )
            if rows:
                dates_map[label] = [r[0] for r in rows]
                data[label] = np.array([float(r[1]) for r in rows])

        if "current_account" not in data or len(data["current_account"]) < 10:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"Insufficient current account data for {country}",
            }

        ca = data["current_account"]
        results: dict = {"country": country, "n_obs": len(ca)}

        # --- 1. Current account persistence (AR(1)) ---
        if len(ca) >= 10:
            ca_lag = ca[:-1]
            ca_curr = ca[1:]
            slope, intercept, r_val, p_val, se = stats.linregress(ca_lag, ca_curr)
            persistence = {
                "ar1_coefficient": round(float(slope), 4),
                "r_squared": round(r_val ** 2, 4),
                "p_value": round(float(p_val), 4),
                "persistent": float(slope) > 0.7,
            }
        else:
            persistence = {"note": "insufficient data"}
            slope = 0.5

        results["persistence"] = persistence

        # --- 2. Chinn-Prasad savings-investment regression ---
        if "gross_savings" in data and "gross_invest" in data:
            sa = data["gross_savings"]
            inv = data["gross_invest"]
            n_cp = min(len(ca), len(sa), len(inv))
            if n_cp >= 10:
                ca_dep = ca[-n_cp:]
                si_surplus = sa[-n_cp:] - inv[-n_cp:]  # savings-investment balance

                regressors = [np.ones(n_cp), si_surplus]
                labels_cp = ["constant", "s_minus_i"]

                if "fiscal_balance" in data and len(data["fiscal_balance"]) >= n_cp:
                    regressors.append(data["fiscal_balance"][-n_cp:])
                    labels_cp.append("fiscal_balance")
                if "nfa" in data and len(data["nfa"]) >= n_cp:
                    regressors.append(data["nfa"][-n_cp:])
                    labels_cp.append("nfa")
                if "fin_develop" in data and len(data["fin_develop"]) >= n_cp:
                    regressors.append(data["fin_develop"][-n_cp:])
                    labels_cp.append("fin_develop")

                X_cp = np.column_stack(regressors)
                beta_cp = np.linalg.lstsq(X_cp, ca_dep, rcond=None)[0]
                resid_cp = ca_dep - X_cp @ beta_cp
                ss_res = float(np.sum(resid_cp ** 2))
                ss_tot = float(np.sum((ca_dep - np.mean(ca_dep)) ** 2))
                r2_cp = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

                se_cp = np.sqrt(ss_res / max(n_cp - len(labels_cp), 1)
                                * np.diag(np.linalg.pinv(X_cp.T @ X_cp)))

                chinn_prasad = {
                    "coefficients": {k: round(float(v), 4)
                                     for k, v in zip(labels_cp, beta_cp)},
                    "std_errors": {k: round(float(v), 4)
                                   for k, v in zip(labels_cp, se_cp)},
                    "r_squared": round(r2_cp, 4),
                    "n_obs": n_cp,
                }

                # Feldstein-Horioka coefficient: beta on (s-i)
                fh_coeff = float(beta_cp[1]) if len(beta_cp) > 1 else None
                chinn_prasad["fh_interpretation"] = (
                    "High capital mobility" if fh_coeff is not None and fh_coeff < 0.3
                    else "Low capital mobility (savings stay home)"
                )
            else:
                chinn_prasad = {"note": "insufficient overlapping data"}
                fh_coeff = None
        else:
            chinn_prasad = {"note": "savings/investment data unavailable"}
            fh_coeff = None

        results["chinn_prasad"] = chinn_prasad

        # --- 3. Reserve accumulation ---
        if "reserves" in data and len(data["reserves"]) >= 2:
            res = data["reserves"]
            d_res = np.diff(res)
            reserve_accum = {
                "latest_reserves_pct_gdp": round(float(res[-1]), 2),
                "mean_annual_change": round(float(np.mean(d_res)), 4),
                "recent_5yr_avg": round(float(np.mean(d_res[-5:])) if len(d_res) >= 5 else
                                        float(np.mean(d_res)), 4),
                "accumulating": float(np.mean(d_res[-5:] if len(d_res) >= 5 else d_res)) > 0,
            }
        else:
            reserve_accum = {"note": "reserves data unavailable"}

        results["reserve_accumulation"] = reserve_accum

        # --- 4. Twin surpluses/deficits ---
        if "fiscal_balance" in data:
            fb = data["fiscal_balance"]
            n_twin = min(len(ca), len(fb))
            ca_latest = float(ca[-1])
            fb_latest = float(fb[-1])
            twin_surplus = ca_latest > 0 and fb_latest > 0
            twin_deficit = ca_latest < 0 and fb_latest < 0
        else:
            ca_latest = float(ca[-1])
            fb_latest = None
            twin_surplus = False
            twin_deficit = ca_latest < 0

        results["twin_position"] = {
            "current_account_latest": round(ca_latest, 2),
            "fiscal_balance_latest": round(fb_latest, 2) if fb_latest is not None else None,
            "twin_surplus": twin_surplus,
            "twin_deficit": twin_deficit,
            "imbalance_size": round(abs(ca_latest), 2),
        }

        # --- Score ---
        # Large CA imbalance
        ca_penalty = min(abs(ca_latest) * 4, 35)

        # Persistence (AR1 rho)
        ar1 = float(slope) if persistence.get("ar1_coefficient") else 0.5
        persist_penalty = max(0.0, ar1 - 0.5) * 30

        # Reserve accumulation (surplus mercantilist) or depletion (deficit stress)
        res_latest = results["reserve_accumulation"].get("latest_reserves_pct_gdp", 0)
        reserve_penalty = 0.0
        if isinstance(res_latest, (int, float)):
            if res_latest > 30:  # large accumulation -> surplus imbalance
                reserve_penalty = 15
            elif res_latest < 5:  # low reserves -> vulnerability
                reserve_penalty = 20

        # Twin deficit is worse than single
        twin_penalty = 15 if twin_deficit else (10 if twin_surplus else 0)

        score = float(np.clip(ca_penalty + persist_penalty + reserve_penalty + twin_penalty, 0, 100))

        return {"score": round(score, 2), "results": results}
