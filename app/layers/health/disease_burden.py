"""Disease burden analysis: DALYs, epidemiological transition, and Preston curve.

Computes disability-adjusted life years (DALYs) from years of life lost (YLL)
and years lived with disability (YLD). Tracks epidemiological transition from
communicable to non-communicable disease dominance. Decomposes mortality by
cause group. Estimates the Preston (1975) curve relating income to life
expectancy.

Key references:
    Murray, C.J.L. (1994). Quantifying the burden of disease: the technical
        basis for disability-adjusted life years. Bulletin of WHO, 72(3).
    Omran, A. (1971). The epidemiological transition. Milbank Memorial Fund
        Quarterly, 49(4), 509-538.
    Preston, S.H. (1975). The changing relation between mortality and level
        of economic development. Population Studies, 29(2), 231-248.
    Cutler, D., Deaton, A. & Lleras-Muney, A. (2006). The determinants of
        mortality. JEP, 20(3), 97-120.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class DiseaseBurden(LayerBase):
    layer_id = "l8"
    name = "Disease Burden"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Compute disease burden metrics and epidemiological transition stage.

        Fetches cause-of-death data, life expectancy, GDP per capita, and
        constructs DALY estimates. Classifies epidemiological transition stage
        and estimates the Preston curve across countries.

        Returns dict with score, DALY estimates, transition stage,
        mortality decomposition, and Preston curve parameters.
        """
        country_iso3 = kwargs.get("country_iso3")

        # Communicable disease mortality (per 1000)
        cd_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.DTH.COMM.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # NCD mortality (per 1000)
        ncd_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.DTH.NCOM.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Injury mortality (per 1000)
        inj_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.DTH.INJR.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Life expectancy
        le_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SP.DYN.LE00.IN'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # GDP per capita (constant USD)
        gdppc_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.PCAP.KD'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Under-5 mortality rate
        u5mr_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.DYN.MORT'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        if not le_rows or not gdppc_rows:
            return {"score": 50, "results": {"error": "no life expectancy or GDP data"}}

        def _index(rows):
            out: dict[str, dict[str, float]] = {}
            for r in rows:
                out.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]
            return out

        cd_data = _index(cd_rows) if cd_rows else {}
        ncd_data = _index(ncd_rows) if ncd_rows else {}
        inj_data = _index(inj_rows) if inj_rows else {}
        le_data = _index(le_rows)
        gdppc_data = _index(gdppc_rows)
        u5mr_data = _index(u5mr_rows) if u5mr_rows else {}

        # --- DALY approximation ---
        # Simplified: DALY = YLL + YLD
        # YLL approximated from mortality rates and standard life expectancy (86.6)
        # YLD approximated from disease prevalence weights
        standard_le = 86.6  # GBD reference life table
        daly_results = None

        if country_iso3 and country_iso3 in le_data:
            le_years = le_data[country_iso3]
            latest_yr = sorted(le_years.keys())[-1]
            current_le = le_years[latest_yr]

            # YLL: (standard_le - actual_le) * crude death rate proxy
            yll_per_capita = max(0, standard_le - current_le)

            # YLD: approximate from NCD prevalence (assume NCD disability weight ~0.2)
            ncd_share = 0
            if country_iso3 in ncd_data:
                ncd_years = ncd_data[country_iso3]
                if latest_yr in ncd_years:
                    ncd_share = ncd_years[latest_yr] / 100.0

            # Rough YLD: NCD share * disability weight * life expectancy
            yld_per_capita = ncd_share * 0.2 * current_le

            daly_per_capita = yll_per_capita + yld_per_capita

            # U5MR contribution to YLL
            u5mr_val = None
            if country_iso3 in u5mr_data:
                u5mr_years = u5mr_data[country_iso3]
                if latest_yr in u5mr_years:
                    u5mr_val = u5mr_years[latest_yr]

            daly_results = {
                "year": latest_yr,
                "daly_per_capita_approx": float(daly_per_capita),
                "yll_per_capita": float(yll_per_capita),
                "yld_per_capita": float(yld_per_capita),
                "life_expectancy": float(current_le),
                "u5mr": float(u5mr_val) if u5mr_val is not None else None,
            }

        # --- Epidemiological transition ---
        transition = None
        if country_iso3:
            cd_ts = cd_data.get(country_iso3, {})
            ncd_ts = ncd_data.get(country_iso3, {})
            common_years = sorted(set(cd_ts.keys()) & set(ncd_ts.keys()))

            if common_years:
                latest_yr = common_years[-1]
                cd_share = cd_ts[latest_yr]
                ncd_share_val = ncd_ts[latest_yr]

                # Classify Omran transition stage
                if cd_share > 50:
                    stage = "age_of_pestilence"  # Stage 1
                    stage_num = 1
                elif cd_share > 30:
                    stage = "age_of_receding_pandemics"  # Stage 2
                    stage_num = 2
                elif ncd_share_val > 70:
                    stage = "age_of_degenerative_diseases"  # Stage 3
                    stage_num = 3
                else:
                    stage = "age_of_delayed_degenerative"  # Stage 4
                    stage_num = 4

                # Transition velocity: change in NCD share over time
                velocity = None
                if len(common_years) >= 5:
                    early_yrs = common_years[:3]
                    late_yrs = common_years[-3:]
                    early_ncd = np.mean([ncd_ts[y] for y in early_yrs])
                    late_ncd = np.mean([ncd_ts[y] for y in late_yrs])
                    years_span = int(late_yrs[-1]) - int(early_yrs[0])
                    if years_span > 0:
                        velocity = float((late_ncd - early_ncd) / years_span)

                transition = {
                    "year": latest_yr,
                    "communicable_share": float(cd_share),
                    "ncd_share": float(ncd_share_val),
                    "stage": stage,
                    "stage_number": stage_num,
                    "transition_velocity": velocity,
                }

        # --- Mortality decomposition ---
        mortality_decomp = None
        if country_iso3:
            cd_ts = cd_data.get(country_iso3, {})
            ncd_ts = ncd_data.get(country_iso3, {})
            inj_ts = inj_data.get(country_iso3, {})
            common = sorted(set(cd_ts.keys()) & set(ncd_ts.keys()) & set(inj_ts.keys()))

            if common:
                latest = common[-1]
                mortality_decomp = {
                    "year": latest,
                    "communicable_pct": float(cd_ts[latest]),
                    "noncommunicable_pct": float(ncd_ts[latest]),
                    "injuries_pct": float(inj_ts[latest]),
                }

        # --- Preston curve ---
        # Cross-country: LE = a + b*log(GDPpc) (log-linear)
        # Also fit: LE = a + b*log(GDPpc) + c*[log(GDPpc)]^2 (quadratic in logs)
        preston = None
        le_list, gdp_list, iso_list = [], [], []
        for iso in set(le_data.keys()) & set(gdppc_data.keys()):
            le_years = le_data[iso]
            gdp_years = gdppc_data[iso]
            common = sorted(set(le_years.keys()) & set(gdp_years.keys()))
            if common:
                yr = common[-1]
                l_val = le_years[yr]
                g_val = gdp_years[yr]
                if l_val and g_val and g_val > 0:
                    le_list.append(l_val)
                    gdp_list.append(g_val)
                    iso_list.append(iso)

        if len(le_list) >= 20:
            le_arr = np.array(le_list)
            log_gdp = np.log(np.array(gdp_list))

            # Log-linear fit
            X_lin = np.column_stack([np.ones(len(log_gdp)), log_gdp])
            beta_lin, _, _, _ = np.linalg.lstsq(X_lin, le_arr, rcond=None)
            y_hat_lin = X_lin @ beta_lin
            ss_res_lin = np.sum((le_arr - y_hat_lin) ** 2)
            ss_tot = np.sum((le_arr - np.mean(le_arr)) ** 2)
            r_sq_lin = 1 - ss_res_lin / ss_tot if ss_tot > 0 else 0

            # Quadratic fit (concavity = diminishing returns)
            X_quad = np.column_stack([np.ones(len(log_gdp)), log_gdp, log_gdp**2])
            beta_quad, _, _, _ = np.linalg.lstsq(X_quad, le_arr, rcond=None)
            y_hat_quad = X_quad @ beta_quad
            ss_res_quad = np.sum((le_arr - y_hat_quad) ** 2)
            r_sq_quad = 1 - ss_res_quad / ss_tot if ss_tot > 0 else 0

            # Residuals for target country
            target_residual = None
            if country_iso3 and country_iso3 in iso_list:
                idx = iso_list.index(country_iso3)
                target_residual = float(le_arr[idx] - y_hat_lin[idx])

            # Countries above and below the curve
            residuals = le_arr - y_hat_lin
            above = [(iso_list[i], float(residuals[i]))
                     for i in np.argsort(-residuals)[:5]]
            below = [(iso_list[i], float(residuals[i]))
                     for i in np.argsort(residuals)[:5]]

            preston = {
                "log_linear": {
                    "intercept": float(beta_lin[0]),
                    "slope": float(beta_lin[1]),
                    "r_squared": float(r_sq_lin),
                },
                "quadratic": {
                    "intercept": float(beta_quad[0]),
                    "linear": float(beta_quad[1]),
                    "quadratic": float(beta_quad[2]),
                    "r_squared": float(r_sq_quad),
                    "concave": bool(beta_quad[2] < 0),
                },
                "n_countries": len(le_list),
                "target_residual": target_residual,
                "overperformers": [{"iso3": c, "residual": round(r, 2)} for c, r in above],
                "underperformers": [{"iso3": c, "residual": round(r, 2)} for c, r in below],
            }

        # --- Score ---
        score = 40
        if daly_results:
            if daly_results["life_expectancy"] < 60:
                score += 30
            elif daly_results["life_expectancy"] < 70:
                score += 15
            if daly_results["u5mr"] and daly_results["u5mr"] > 50:
                score += 15
            elif daly_results["u5mr"] and daly_results["u5mr"] > 20:
                score += 8

        if transition and transition["stage_number"] <= 2:
            score += 10

        if preston and preston["target_residual"] is not None:
            if preston["target_residual"] < -5:
                score += 10  # underperforming the curve

        score = float(np.clip(score, 0, 100))

        results = {
            "daly": daly_results,
            "epidemiological_transition": transition,
            "mortality_decomposition": mortality_decomp,
            "preston_curve": preston,
            "country_iso3": country_iso3,
        }

        return {"score": score, "results": results}
