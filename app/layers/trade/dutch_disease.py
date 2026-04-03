"""Dutch Disease analysis module.

Methodology
-----------
The Dutch Disease refers to the adverse effect of a natural resource boom
on the traded (manufacturing) sector through two mechanisms:

1. **Resource movement effect** (Corden & Neary 1982): labor and capital
   move from manufacturing into the booming resource sector, shrinking
   tradeable production.

2. **Spending effect**: resource windfall raises income -> increased demand
   for non-tradeables -> real exchange rate appreciation -> manufactured
   exports become uncompetitive.

Empirical tests:

1. Real effective exchange rate (REER) response to commodity price
   shocks: OLS regression of DREER on commodity price index changes and
   lagged DREER.

2. Manufacturing employment/output share trend: test whether resource
   booms coincide with manufacturing squeeze (deindustrialization).

3. Corden-Neary model calibration: estimate the splitting of the spending
   effect (share going to non-tradeables vs savings/investment).

4. Resource dependence index: resource exports as % of total merchandise
   exports (WB/UNCTAD). High dependence raises Dutch Disease risk.

Score (0-100): higher score indicates stronger Dutch Disease symptoms --
REER appreciation, shrinking manufacturing, high resource dependence.

References:
    Corden, W.M. and Neary, J.P. (1982). "Booming Sector and
        De-Industrialisation in a Small Open Economy." The Economic
        Journal, 92(368), 825-848.
    Sachs, J.D. and Warner, A.M. (1995). "Natural Resource Abundance and
        Economic Growth." NBER Working Paper 5398.
    Gylfason, T. (2001). "Natural Resources, Education, and Economic
        Development." European Economic Review, 45(4-6), 847-859.
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class DutchDisease(LayerBase):
    layer_id = "l1"
    name = "Dutch Disease"

    async def compute(self, db, **kwargs) -> dict:
        """Compute Dutch Disease indicators.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country          : str   - ISO3 country code
            resource_sector  : str   - 'oil', 'gas', 'minerals', 'agriculture' (default: 'oil')
        """
        country = kwargs.get("country", "NGA")
        resource_sector = kwargs.get("resource_sector", "oil")

        series_map = {
            "reer":          f"REER_{country}",
            "commodity_px":  f"COMMODITY_PRICE_{resource_sector.upper()}",
            "manuf_share":   f"MANUF_VALUE_ADD_GDP_{country}",
            "manuf_emp":     f"MANUF_EMP_SHARE_{country}",
            "resource_exp":  f"RESOURCE_EXPORTS_PCT_{country}",
            "gdp":           f"GDP_CONST_{country}",
        }

        data: dict[str, np.ndarray] = {}
        for label, code in series_map.items():
            rows = await db.execute_fetchall(
                "SELECT date, value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE code = ?) ORDER BY date",
                (code,),
            )
            if rows:
                data[label] = np.array([float(r[1]) for r in rows])

        if "reer" not in data and "resource_exp" not in data:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"Insufficient data for Dutch Disease analysis of {country}",
            }

        results: dict = {"country": country, "resource_sector": resource_sector}

        # --- 1. REER response to commodity price shocks ---
        reer_analysis = {}
        if "reer" in data and "commodity_px" in data:
            reer = data["reer"]
            cpx = data["commodity_px"]
            n_r = min(len(reer), len(cpx))
            if n_r >= 10:
                reer_s = reer[-n_r:]
                cpx_s = cpx[-n_r:]

                # First-difference specification to avoid spurious regression
                d_reer = np.diff(np.log(np.maximum(reer_s, 1e-6)))
                d_cpx = np.diff(np.log(np.maximum(cpx_s, 1e-6)))
                n_d = len(d_reer)

                if n_d >= 8:
                    # OLS: d_reer = alpha + beta * d_cpx + gamma * d_reer_lag + e
                    X_reer = np.column_stack([
                        np.ones(n_d - 1),
                        d_cpx[1:],
                        d_reer[:-1],  # lagged REER change
                    ])
                    y_reer = d_reer[1:]
                    beta_r = np.linalg.lstsq(X_reer, y_reer, rcond=None)[0]
                    resid_r = y_reer - X_reer @ beta_r
                    ss_res = float(np.sum(resid_r ** 2))
                    ss_tot = float(np.sum((y_reer - np.mean(y_reer)) ** 2))
                    r2_r = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

                    commodity_beta = float(beta_r[1])
                    t_stat_r = commodity_beta / (
                        np.sqrt(ss_res / max(len(y_reer) - 3, 1)
                                * np.linalg.pinv(X_reer.T @ X_reer)[1, 1])
                    ) if ss_tot > 0 else 0.0

                    reer_analysis = {
                        "commodity_beta": round(commodity_beta, 4),
                        "t_statistic": round(float(t_stat_r), 3),
                        "r_squared": round(r2_r, 4),
                        "n_obs": len(y_reer),
                        "reer_appreciation": commodity_beta > 0,
                        "significant": abs(t_stat_r) > 1.96,
                    }
                else:
                    reer_analysis = {"note": "insufficient differenced obs"}
                    commodity_beta = 0.0
            else:
                reer_analysis = {"note": "short series"}
                commodity_beta = 0.0
        else:
            reer_analysis = {"note": "REER or commodity price data unavailable"}
            commodity_beta = 0.0

        results["reer_response"] = reer_analysis

        # --- 2. Manufacturing squeeze ---
        manuf_analysis = {}
        manuf_squeeze = False
        if "manuf_share" in data and len(data["manuf_share"]) >= 5:
            ms = data["manuf_share"]
            x_t = np.arange(len(ms), dtype=float)
            slope_m, intercept_m, r_m, p_m, _ = stats.linregress(x_t, ms)
            manuf_analysis = {
                "latest_manuf_share": round(float(ms[-1]), 2),
                "trend_slope_annual": round(float(slope_m), 4),
                "r_squared": round(r_m ** 2, 4),
                "p_value": round(float(p_m), 4),
                "deindustrializing": float(slope_m) < -0.2,
            }
            manuf_squeeze = float(slope_m) < -0.2
        else:
            manuf_analysis = {"note": "manufacturing data unavailable"}

        if "manuf_emp" in data and len(data["manuf_emp"]) >= 5:
            me = data["manuf_emp"]
            slope_e, _, r_e, p_e, _ = stats.linregress(np.arange(len(me), dtype=float), me)
            manuf_analysis["employment"] = {
                "latest_manuf_emp_share": round(float(me[-1]), 2),
                "trend_slope": round(float(slope_e), 4),
                "r_squared": round(r_e ** 2, 4),
                "shrinking": float(slope_e) < -0.1,
            }

        results["manufacturing_squeeze"] = manuf_analysis

        # --- 3. Corden-Neary model calibration ---
        # Simple two-sector spending effect calibration:
        # Spending effect = theta * resource_windfall
        # Split: delta_NT = theta_NT * windfall, delta_T = (1-theta_NT) * windfall
        # We estimate theta_NT from the regression of non-tradeable price index
        # on resource revenue changes.
        cn_model = {}
        if "reer" in data and len(data["reer"]) >= 5:
            reer_arr = data["reer"]
            # REER appreciation as proxy for non-tradeable inflation
            reer_chg = float(np.mean(np.diff(np.log(np.maximum(reer_arr[-10:], 1e-6))))) \
                if len(reer_arr) >= 10 else 0.0

            # Heuristic: spending effect share going to non-tradeables
            # calibrated from aggregate REER passthrough
            if commodity_beta > 0:
                theta_nt = float(np.clip(commodity_beta, 0.0, 1.0))
            else:
                theta_nt = 0.0

            cn_model = {
                "spending_effect_nt_share": round(theta_nt, 4),
                "resource_movement_effect": "present" if manuf_squeeze else "absent",
                "reer_trend_log_annual": round(reer_chg, 6),
                "corden_neary_risk": "high" if theta_nt > 0.4 and manuf_squeeze else
                                    "moderate" if theta_nt > 0.2 else "low",
            }
        else:
            cn_model = {"note": "insufficient data for Corden-Neary calibration"}

        results["corden_neary"] = cn_model

        # --- 4. Resource dependence ---
        resource_dep_val = 0.0
        if "resource_exp" in data and len(data["resource_exp"]) >= 1:
            resource_dep_val = float(data["resource_exp"][-1])
            resource_dep = {
                "resource_exports_pct": round(resource_dep_val, 2),
                "highly_dependent": resource_dep_val > 50,
                "moderately_dependent": 20 < resource_dep_val <= 50,
            }
        else:
            resource_dep = {"note": "resource export data unavailable"}

        results["resource_dependence"] = resource_dep

        # --- Score ---
        # REER appreciation from commodity beta
        reer_penalty = min(float(max(0, commodity_beta)) * 40, 30)

        # Manufacturing squeeze
        manuf_penalty = 25 if manuf_squeeze else 0

        # Resource dependence
        if resource_dep_val > 50:
            dep_penalty = 30
        elif resource_dep_val > 20:
            dep_penalty = 15
        else:
            dep_penalty = 5

        # Corden-Neary spending effect
        cn_risk = cn_model.get("corden_neary_risk", "low")
        cn_penalty = 15 if cn_risk == "high" else (8 if cn_risk == "moderate" else 0)

        score = float(np.clip(reer_penalty + manuf_penalty + dep_penalty + cn_penalty, 0, 100))

        return {"score": round(score, 2), "results": results}
