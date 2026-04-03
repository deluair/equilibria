"""Creative destruction: Schumpeterian dynamics, Olley-Pakes reallocation, zombie firms.

Methodology
-----------
1. **Schumpeterian Entry-Exit Dynamics**:
   Turbulence index = (entry rate + exit rate) / 2.
   Excess entry (entry - exit) signals market dynamism or overshooting.
   Innovation-adjusted Schumpeter mark: high turbulence in high-TFP sectors
   indicates creative destruction (vs. churning without productivity effect).
   Reference: Aghion-Howitt (1992) quality ladders model.

2. **Olley-Pakes Productivity Decomposition**:
   Aggregate productivity P_t = sum_i s_{it} * p_{it}
   Olley-Pakes (1996) decomposition:
     P_t = P_bar_t + cov_t(s_{it}, p_{it})
   where P_bar = unweighted mean TFP, cov = allocation efficiency term.
   cov > 0 means high-productivity firms have larger market shares (efficient).
   Change in cov over time measures reallocation gains.

3. **Zombie Firm Prevalence**:
   Andrews-Petroulakis (2019) / McGowan-Andrews-Millot (2017) definition:
   Zombie = firm older than 10 years with interest coverage ratio < 1
   for 3 consecutive years (cannot service debt from operating profits).
   ICR = EBIT / interest_expense.
   Zombie share of sector employment: > 10% signals resource misallocation.
   Caballero-Hoshi-Kashyap (2008): zombie lending distorts entry of healthy firms.

4. **Cleansing Effect of Recessions**:
   Caballero-Hammour (1994): recessions reallocate labor from low to high
   productivity uses, raising long-run efficiency.
   Empirical test: correlation between output contraction and TFP growth.
   Anti-cleansing: if zombie firms survive recessions (credit-driven), productivity
   gains are forgone. Measured as excess zombie survival rate during downturns.

References:
    Aghion, P. & Howitt, P. (1992). A Model of Growth Through Creative Destruction.
        Econometrica 60(2): 323-351.
    Olley, G.S. & Pakes, A. (1996). The Dynamics of Productivity in the
        Telecommunications Equipment Industry. Econometrica 64(6): 1263-1297.
    McGowan, M.A., Andrews, D. & Millot, V. (2017). The Walking Dead? Zombie
        Firms and Productivity Performance in OECD Countries. OECD Working Paper.
    Caballero, R. & Hammour, M. (1994). The Cleansing Effect of Recessions.
        AER 84(5): 1350-1368.

Score: high zombie share + poor reallocation + low turbulence -> STRESS.
"""

from __future__ import annotations

import json

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class CreativeDestruction(LayerBase):
    layer_id = "l14"
    name = "Creative Destruction"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")
        lookback = kwargs.get("lookback_years", 15)

        series_map = {
            "entry_rate": f"FIRM_ENTRY_RATE_{country}",
            "exit_rate": f"FIRM_EXIT_RATE_{country}",
            "tfp_growth": f"TFP_GROWTH_{country}",
            "zombie_share": f"ZOMBIE_FIRM_SHARE_{country}",
            "zombie_employment": f"ZOMBIE_EMPLOYMENT_SHARE_{country}",
            "icr": f"INTEREST_COVERAGE_RATIO_{country}",
            "gdp_growth": f"GDP_GROWTH_{country}",
            "op_cov": f"OP_COVARIANCE_{country}",
        }

        data: dict[str, dict[str, float]] = {}
        for label, code in series_map.items():
            rows = await db.execute_fetchall(
                "SELECT date, value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE series_id = ?) "
                "AND date >= date('now', ?) ORDER BY date",
                (code, f"-{lookback} years"),
            )
            if rows:
                data[label] = {r[0]: float(r[1]) for r in rows}

        results: dict = {"country": country, "lookback_years": lookback}
        has_any = False

        # --- 1. Schumpeterian Entry-Exit Dynamics ---
        if data.get("entry_rate") and data.get("exit_rate"):
            has_any = True
            common = sorted(set(data["entry_rate"]) & set(data["exit_rate"]))
            if common:
                entry = np.array([data["entry_rate"][d] for d in common])
                exit_ = np.array([data["exit_rate"][d] for d in common])
                turbulence = (entry + exit_) / 2.0
                excess_entry = entry - exit_
                results["schumpeter_dynamics"] = {
                    "turbulence_index_latest": round(float(turbulence[-1]), 4),
                    "turbulence_mean": round(float(np.mean(turbulence)), 4),
                    "excess_entry_latest": round(float(excess_entry[-1]), 4),
                    "entry_exit_balance": "entry_dominant" if excess_entry[-1] > 0 else "exit_dominant",
                    "high_turbulence": bool(turbulence[-1] > float(np.mean(turbulence))),
                    "n_obs": len(common),
                }
        elif data.get("entry_rate") or data.get("exit_rate"):
            has_any = True
            key = "entry_rate" if data.get("entry_rate") else "exit_rate"
            dates = sorted(data[key])
            vals = np.array([data[key][d] for d in dates])
            results["schumpeter_dynamics"] = {
                f"{key}_latest": round(float(vals[-1]), 4),
                f"{key}_mean": round(float(np.mean(vals)), 4),
                "note": "only partial dynamics data available",
            }

        # --- 2. Olley-Pakes Decomposition ---
        if data.get("op_cov"):
            has_any = True
            cov_dates = sorted(data["op_cov"])
            cov_vals = np.array([data["op_cov"][d] for d in cov_dates])
            cov_trend = float(np.polyfit(np.arange(len(cov_vals)), cov_vals, 1)[0])
            results["olley_pakes"] = {
                "allocation_covariance_latest": round(float(cov_vals[-1]), 4),
                "allocation_covariance_mean": round(float(np.mean(cov_vals)), 4),
                "reallocation_trend": round(cov_trend, 6),
                "efficient_allocation": bool(cov_vals[-1] > 0),
                "improving": bool(cov_trend > 0),
                "n_obs": len(cov_vals),
            }

        # --- 3. Zombie Firm Prevalence ---
        zombie_data: dict = {}
        if data.get("zombie_share"):
            has_any = True
            z_dates = sorted(data["zombie_share"])
            z_vals = np.array([data["zombie_share"][d] for d in z_dates])
            zombie_data["zombie_share_latest"] = round(float(z_vals[-1]), 4)
            zombie_data["zombie_share_mean"] = round(float(np.mean(z_vals)), 4)
            zombie_data["zombie_trend"] = round(float(np.polyfit(np.arange(len(z_vals)), z_vals, 1)[0]), 6)
            zombie_data["high_zombie_prevalence"] = bool(z_vals[-1] > 0.10)

        if data.get("zombie_employment"):
            has_any = True
            ze_dates = sorted(data["zombie_employment"])
            ze_vals = np.array([data["zombie_employment"][d] for d in ze_dates])
            zombie_data["zombie_employment_share_latest"] = round(float(ze_vals[-1]), 4)
            zombie_data["zombie_employment_share_mean"] = round(float(np.mean(ze_vals)), 4)
            zombie_data["employment_misallocation"] = bool(ze_vals[-1] > 0.10)

        if data.get("icr"):
            has_any = True
            icr_dates = sorted(data["icr"])
            icr_vals = np.array([data["icr"][d] for d in icr_dates])
            zombie_data["mean_icr"] = round(float(np.mean(icr_vals)), 4)
            zombie_data["share_below_1"] = round(float(np.mean(icr_vals < 1.0)), 4)

        if zombie_data:
            results["zombie_firms"] = zombie_data

        # --- 4. Cleansing Effect ---
        if data.get("gdp_growth") and data.get("tfp_growth"):
            has_any = True
            common_gdp = sorted(set(data["gdp_growth"]) & set(data["tfp_growth"]))
            if len(common_gdp) >= 6:
                gdp = np.array([data["gdp_growth"][d] for d in common_gdp])
                tfp = np.array([data["tfp_growth"][d] for d in common_gdp])

                # Cleansing: correlation between GDP contraction and TFP gain
                recessions = gdp < 0
                cleansing_corr = float(np.corrcoef(-gdp, tfp)[0, 1]) if len(gdp) > 2 else None
                # Anti-cleansing: zombies survive recessions
                if data.get("zombie_share"):
                    rec_z = []
                    exp_z = []
                    for d in common_gdp:
                        if d in data["zombie_share"]:
                            if data["gdp_growth"][d] < 0:
                                rec_z.append(data["zombie_share"][d])
                            else:
                                exp_z.append(data["zombie_share"][d])
                    zombie_recession_mean = float(np.mean(rec_z)) if rec_z else None
                    zombie_expansion_mean = float(np.mean(exp_z)) if exp_z else None
                    anti_cleansing = (
                        zombie_recession_mean is not None
                        and zombie_expansion_mean is not None
                        and zombie_recession_mean >= zombie_expansion_mean
                    )
                else:
                    zombie_recession_mean = zombie_expansion_mean = None
                    anti_cleansing = None

                results["cleansing_effect"] = {
                    "gdp_tfp_correlation": round(cleansing_corr, 4) if cleansing_corr is not None else None,
                    "cleansing_present": cleansing_corr > 0.2 if cleansing_corr is not None else None,
                    "n_recession_periods": int(np.sum(recessions)),
                    "zombie_recession_mean": round(zombie_recession_mean, 4) if zombie_recession_mean is not None else None,
                    "zombie_expansion_mean": round(zombie_expansion_mean, 4) if zombie_expansion_mean is not None else None,
                    "anti_cleansing": anti_cleansing,
                }

        if not has_any:
            return {"score": 50.0, "results": {"error": "no creative destruction data available"}}

        # --- Score ---
        stress_score = 0.0

        sd = results.get("schumpeter_dynamics", {})
        if sd.get("turbulence_mean") is not None and sd.get("turbulence_index_latest") is not None:
            if float(sd["turbulence_index_latest"]) < float(sd["turbulence_mean"]) * 0.8:
                stress_score += 15.0

        op = results.get("olley_pakes", {})
        if op.get("efficient_allocation") is False:
            stress_score += 20.0
        if op.get("improving") is False and op.get("allocation_covariance_mean") is not None:
            if float(op["allocation_covariance_mean"]) < 0.05:
                stress_score += 10.0

        zf = results.get("zombie_firms", {})
        if zf.get("high_zombie_prevalence"):
            stress_score += 25.0
        if zf.get("employment_misallocation"):
            stress_score += 15.0

        ce = results.get("cleansing_effect", {})
        if ce.get("anti_cleansing"):
            stress_score += 15.0

        score = max(0.0, min(100.0, stress_score))

        return {"score": round(score, 1), "results": results}
