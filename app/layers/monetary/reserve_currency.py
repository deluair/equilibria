"""Reserve currency: share dynamics, exorbitant privilege, Triffin dilemma, RMB internationalization.

Methodology
-----------
1. **Reserve Currency Share Dynamics**:
   COFER (IMF Currency Composition of Foreign Exchange Reserves) data.
   Reserve share of currency i at time t: rs_i(t) = R_i / sum_j(R_j)
   Share dynamics model (Eichengreen-Frankel 1996): network externalities create
   persistence. Transition model: delta_rs = alpha + beta * rs_{t-1} + gamma * Z
   where Z = relative economic size, financial depth, geopolitical factors.
   HHI of reserve diversification: low HHI = diversified (away from USD dominance).

2. **Exorbitant Privilege Estimation**:
   Gourinchas-Rey (2007): US earns excess returns on foreign assets vs. liabilities.
   Exorbitant privilege = r_assets - r_liabilities (return differential).
   Decomposition: composition effect (equity vs. debt) + return differential.
   Seigniorage component: foreigners hold USD cash -> free loan to US Treasury.
   Annual seigniorage = USD_currency_held_abroad * federal_funds_rate.
   "Dark matter": US trade deficit overstates actual net external liabilities.

3. **Triffin Dilemma**:
   Triffin (1960): reserve currency country must run deficits to supply liquidity,
   but deficits undermine confidence in the currency's value.
   Modern Triffin: global demand for safe assets > supply from reserve issuer.
   Triffin tension index = (global_reserves / GDP_issuer) / (AAA_debt / GDP_issuer)
   High ratio: demand outstrips supply -> SDR or multi-polar alternatives needed.
   Obstfeld-Rogoff (2009) imbalances link: US CA deficit as global reserve provision.

4. **Renminbi Internationalization**:
   SWIFT RMB share in international payments. BIS triennial survey: RMB FX turnover.
   SWIFT usage index: RMB share of SWIFT messages.
   Chinn-Frankel (2007) gravity-based currency internalization model:
     ln(reserve_share) = a + b*ln(GDP_share) + c*ln(trade_share) + d*FX_liquidity + e
   RMB internationalization score vs. model-predicted share given China's economic size.

References:
    Gourinchas, P.-O. & Rey, H. (2007). From World Banker to World Venture
        Capitalist: US External Adjustment and the Exorbitant Privilege.
        In Clarida (Ed.), G7 Current Account Imbalances. NBER/University of Chicago.
    Triffin, R. (1960). Gold and the Dollar Crisis. Yale University Press.
    Eichengreen, B. & Frankel, J. (1996). The SDR, Reserve Currencies, and the
        Future of the Bretton Woods System. In Bordo & Eichengreen (Eds.).
    Chinn, M. & Frankel, J. (2007). Will the Euro Eventually Surpass the Dollar
        as Leading International Reserve Currency? In Clarida (Ed.).

Score: high reserve concentration (USD dominance) + Triffin tension + low RMB -> watch.
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class ReserveCurrency(LayerBase):
    layer_id = "l15"
    name = "Reserve Currency"

    async def compute(self, db, **kwargs) -> dict:
        lookback = kwargs.get("lookback_years", 25)
        focal_currency = kwargs.get("focal_currency", "USD")

        series_map = {
            "usd_share": "COFER_USD_SHARE",
            "eur_share": "COFER_EUR_SHARE",
            "rmb_share": "COFER_RMB_SHARE",
            "gbp_share": "COFER_GBP_SHARE",
            "jpy_share": "COFER_JPY_SHARE",
            "total_reserves": "IMF_TOTAL_RESERVES_USD",
            "us_ca_deficit": "US_CURRENT_ACCOUNT_GDP",
            "us_aaa_debt": "US_AAA_DEBT_GDP",
            "us_ext_assets_return": "US_EXT_ASSETS_RETURN",
            "us_ext_liab_return": "US_EXT_LIAB_RETURN",
            "rmb_swift_share": "SWIFT_RMB_SHARE",
            "us_gdp_share": "US_GDP_WORLD_SHARE",
            "china_gdp_share": "CHINA_GDP_WORLD_SHARE",
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

        results: dict = {"focal_currency": focal_currency, "lookback_years": lookback}
        has_any = False

        # --- 1. Reserve Share Dynamics ---
        share_series = {k: data[k] for k in ["usd_share", "eur_share", "rmb_share", "gbp_share", "jpy_share"] if data.get(k)}
        if share_series:
            has_any = True
            # Get latest snapshot across all available currencies
            latest_shares: dict[str, float] = {}
            trends: dict[str, float] = {}
            for ccy, series in share_series.items():
                dates = sorted(series)
                vals = np.array([series[d] for d in dates])
                latest_shares[ccy] = round(float(vals[-1]), 4)
                if len(vals) >= 5:
                    trends[ccy] = round(float(np.polyfit(np.arange(len(vals)), vals, 1)[0]), 6)

            # HHI of reserve diversification
            s_arr = np.array(list(latest_shares.values()), dtype=float)
            s_arr = s_arr / s_arr.sum() if s_arr.sum() > 0 else s_arr
            hhi = float(np.sum(s_arr ** 2))

            reserve_dynamics: dict = {
                "latest_shares": latest_shares,
                "share_trends": trends,
                "reserve_hhi": round(hhi, 4),
                "diversification_level": (
                    "concentrated" if hhi > 0.5
                    else "moderate" if hhi > 0.3
                    else "diversified"
                ),
            }

            if data.get("usd_share"):
                usd_dates = sorted(data["usd_share"])
                usd = np.array([data["usd_share"][d] for d in usd_dates])
                if len(usd) >= 10:
                    # AR(1) for persistence
                    ar1 = float(np.corrcoef(usd[:-1], usd[1:])[0, 1]) if len(usd) > 3 else None
                    reserve_dynamics["usd_persistence_ar1"] = round(ar1, 4) if ar1 is not None else None

            results["reserve_dynamics"] = reserve_dynamics

        # --- 2. Exorbitant Privilege ---
        if data.get("us_ext_assets_return") and data.get("us_ext_liab_return"):
            has_any = True
            common_r = sorted(set(data["us_ext_assets_return"]) & set(data["us_ext_liab_return"]))
            if common_r:
                r_assets = np.array([data["us_ext_assets_return"][d] for d in common_r])
                r_liab = np.array([data["us_ext_liab_return"][d] for d in common_r])
                privilege = r_assets - r_liab
                mean_privilege = float(np.mean(privilege))
                current_privilege = float(privilege[-1])

                # Statistical significance
                if len(privilege) >= 5:
                    t_stat, p_val = sp_stats.ttest_1samp(privilege, 0.0)
                else:
                    t_stat, p_val = 0.0, 1.0

                results["exorbitant_privilege"] = {
                    "return_differential_latest_pp": round(current_privilege * 100, 2),
                    "return_differential_mean_pp": round(mean_privilege * 100, 2),
                    "t_statistic": round(float(t_stat), 3),
                    "p_value": round(float(p_val), 4),
                    "statistically_significant": float(p_val) < 0.05,
                    "n_obs": len(common_r),
                }

        # --- 3. Triffin Dilemma ---
        if data.get("total_reserves") and data.get("us_aaa_debt"):
            has_any = True
            common_t = sorted(set(data["total_reserves"]) & set(data["us_aaa_debt"]))
            if common_t:
                tot_res = np.array([data["total_reserves"][d] for d in common_t])
                aaa = np.array([data["us_aaa_debt"][d] for d in common_t])

                triffin_ratio = tot_res / np.maximum(aaa, 1e-10)
                current_ratio = float(triffin_ratio[-1])
                trend_triffin = float(np.polyfit(np.arange(len(triffin_ratio)), triffin_ratio, 1)[0])

                ca_tension = None
                if data.get("us_ca_deficit"):
                    ca_dates = sorted(data["us_ca_deficit"])
                    ca = np.array([data["us_ca_deficit"][d] for d in ca_dates])
                    ca_tension = float(ca[-1])

                results["triffin_dilemma"] = {
                    "triffin_tension_index": round(current_ratio, 4),
                    "triffin_trend": round(trend_triffin, 6),
                    "tension_increasing": bool(trend_triffin > 0),
                    "us_ca_gdp_latest": round(ca_tension, 4) if ca_tension is not None else None,
                    "tension_level": (
                        "high" if current_ratio > 1.5
                        else "moderate" if current_ratio > 1.0
                        else "low"
                    ),
                    "n_obs": len(common_t),
                }

        # --- 4. RMB Internationalization ---
        rmb_data: dict = {}
        if data.get("rmb_swift_share"):
            has_any = True
            rs_dates = sorted(data["rmb_swift_share"])
            rs = np.array([data["rmb_swift_share"][d] for d in rs_dates])
            rmb_data["swift_share_latest_pct"] = round(float(rs[-1]) * 100, 2)
            rmb_data["swift_share_trend"] = round(float(np.polyfit(np.arange(len(rs)), rs, 1)[0]) * 100, 6)

        if data.get("rmb_share") and data.get("china_gdp_share"):
            has_any = True
            common_g = sorted(set(data["rmb_share"]) & set(data["china_gdp_share"]))
            if len(common_g) >= 5:
                rmb_res = np.array([data["rmb_share"][d] for d in common_g])
                gdp_sh = np.array([data["china_gdp_share"][d] for d in common_g])
                # Model: ln(reserve_share) ~ ln(gdp_share)
                if np.std(np.log(np.maximum(gdp_sh, 1e-10)), ddof=1) > 1e-10:
                    X = np.column_stack([np.ones(len(common_g)), np.log(np.maximum(gdp_sh, 1e-10))])
                    beta = np.linalg.lstsq(X, np.log(np.maximum(rmb_res, 1e-10)), rcond=None)[0]
                    fitted_rmb = float(np.exp(X[-1] @ beta))
                    actual_rmb = float(rmb_res[-1])
                    internalization_gap = actual_rmb - fitted_rmb
                    rmb_data["model_predicted_share"] = round(fitted_rmb, 4)
                    rmb_data["actual_share_latest"] = round(actual_rmb, 4)
                    rmb_data["internationalization_gap"] = round(internalization_gap, 4)
                    rmb_data["under_internationalized"] = internalization_gap < -0.01

        if rmb_data:
            results["rmb_internationalization"] = rmb_data

        if not has_any:
            return {"score": 50.0, "results": {"error": "no reserve currency data available"}}

        # --- Score ---
        # For reserve currency module, high USD concentration is a systemic fragility indicator
        # High Triffin tension and low RMB internationalization = imbalanced system -> WATCH/STRESS
        stress = 10.0

        rd = results.get("reserve_dynamics", {})
        if rd.get("diversification_level") == "concentrated":
            stress += 20.0
        elif rd.get("diversification_level") == "moderate":
            stress += 10.0

        td = results.get("triffin_dilemma", {})
        if td.get("tension_level") == "high":
            stress += 25.0
        elif td.get("tension_level") == "moderate":
            stress += 12.0
        if td.get("tension_increasing"):
            stress += 10.0

        ep = results.get("exorbitant_privilege", {})
        # Large privilege signals structural imbalance
        if ep.get("return_differential_mean_pp") is not None:
            diff = abs(float(ep["return_differential_mean_pp"]))
            if diff > 2.0:
                stress += min(diff * 2.0, 15.0)

        rmb = results.get("rmb_internationalization", {})
        if rmb.get("under_internationalized"):
            stress += 10.0

        score = max(0.0, min(100.0, stress))

        return {"score": round(score, 1), "results": results}
