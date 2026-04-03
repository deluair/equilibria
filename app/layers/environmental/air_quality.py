"""Air quality economics: PM2.5 health costs, EKC, clean air benefits, transboundary.

Estimates the economic health burden of PM2.5 exposure using the WHO Global
Burden of Disease concentration-response framework. Tests for an environmental
Kuznets curve (EKC) relationship between income and air pollution. Evaluates
the economic benefits of clean air regulation using cost-benefit analysis.
Quantifies cross-border pollution externalities using a gravity-decay model.

Key references:
    WHO (2021). Global Air Quality Guidelines: Particulate Matter, Ozone,
        Nitrogen Dioxide, Sulfur Dioxide and Carbon Monoxide. Geneva.
    Burnett, R. et al. (2018). Global estimates of mortality associated with
        long-term exposure to outdoor fine particle pollution. PNAS, 115(38).
    Selden, T.M. & Song, D. (1994). Environmental quality and development: is
        there a Kuznets curve for air pollution? Journal of Environmental
        Economics and Management, 27(2), 147-162.
    Muller, N.Z. & Mendelsohn, R. (2009). Efficient pollution regulation.
        AER, 99(5), 1714-1739.
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class AirQuality(LayerBase):
    layer_id = "l9"
    name = "Air Quality Economics"
    weight = 0.20

    # WHO PM2.5 annual guideline: 5 ug/m3 (2021); interim targets: 10, 15, 25 ug/m3
    WHO_PM25_GUIDELINE = 5.0
    WHO_PM25_IT1 = 35.0   # Interim Target 1 (most lenient)
    WHO_PM25_IT2 = 25.0
    WHO_PM25_IT3 = 15.0
    WHO_PM25_SAFE = 10.0  # IT4

    # Concentration-response coefficient (Burnett et al. 2018 GEMM)
    # log-linear approximation: RR = exp(beta * ln(1 + C/alpha))
    GEMM_BETA = 0.137   # theta (non-linear) coefficient
    GEMM_ALPHA = 2.4    # shape parameter

    async def compute(self, db, **kwargs) -> dict:
        """Compute air quality economic burden and policy benefit estimates.

        Fetches GDP, population, CO2 emissions, and energy data as air quality
        proxies. Estimates PM2.5 health cost, tests EKC, computes regulation
        benefit-cost analysis, and cross-border externalities.

        Returns dict with score, pm25_health_cost, ekc_analysis, clean_air_cba,
        and transboundary_externalities.
        """
        country_iso3 = kwargs.get("country_iso3")

        # GDP per capita
        gdppc_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.PCAP.KD'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # GDP total
        gdp_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.MKTP.KD'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Population
        pop_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SP.POP.TOTL'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # CO2 emissions per capita (metric tonnes) - proxy for air pollution intensity
        co2pc_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'EN.ATM.CO2E.PC'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # CO2 total (kt)
        co2_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'EN.ATM.CO2E.KT'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Energy use per capita (kg oil eq) - combustion = PM2.5 source
        energy_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'EG.USE.PCAP.KG.OE'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Urban population % - urban areas have higher PM2.5
        urban_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SP.URB.TOTL.IN.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        if not gdppc_rows:
            return {"score": 50, "results": {"error": "no GDP data"}}

        def _index(rows):
            out: dict[str, dict[str, float]] = {}
            for r in rows:
                out.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]
            return out

        gdppc_data = _index(gdppc_rows)
        gdp_data = _index(gdp_rows) if gdp_rows else {}
        pop_data = _index(pop_rows) if pop_rows else {}
        co2pc_data = _index(co2pc_rows) if co2pc_rows else {}
        co2_data = _index(co2_rows) if co2_rows else {}
        energy_data = _index(energy_rows) if energy_rows else {}
        urban_data = _index(urban_rows) if urban_rows else {}

        pm25_health_cost = None
        ekc_analysis = None
        clean_air_cba = None
        transboundary = None

        target = country_iso3
        gdppc_ts = gdppc_data.get(target, {}) if target else {}
        gdp_ts = gdp_data.get(target, {}) if target else {}
        pop_ts = pop_data.get(target, {}) if target else {}
        co2pc_ts = co2pc_data.get(target, {}) if target else {}
        energy_ts = energy_data.get(target, {}) if target else {}
        urban_ts = urban_data.get(target, {}) if target else {}

        if gdppc_ts and pop_ts:
            latest_yr = sorted(set(gdppc_ts.keys()) & set(pop_ts.keys()))[-1]
            gdppc = gdppc_ts[latest_yr]
            pop = pop_ts[latest_yr]
            gdp_val = gdp_ts.get(latest_yr, gdppc * pop) if gdp_ts else gdppc * pop

            # Estimate PM2.5 from income and energy use proxy
            # IQAir 2022: global weighted mean PM2.5 ~32 ug/m3
            # Strong income gradient (Selden & Song 1994 EKC)
            co2pc_val = co2pc_ts.get(latest_yr) if co2pc_ts else None
            energy_val = energy_ts.get(latest_yr) if energy_ts else None
            urban_pct = urban_ts.get(latest_yr) if urban_ts else 55.0

            # PM2.5 estimate: combine income-based EKC with energy proxy
            # EKC turning point ~$12,000 GDPpc (Selden & Song)
            if gdppc < 1000:
                pm25_est = 45.0   # very high: biomass burning, poor sanitation
            elif gdppc < 3000:
                pm25_est = 55.0   # industrialization peak
            elif gdppc < 8000:
                pm25_est = 40.0   # rapid growth but some regulation
            elif gdppc < 15000:
                pm25_est = 22.0   # EKC turning point region
            elif gdppc < 30000:
                pm25_est = 12.0
            else:
                pm25_est = 7.0    # post-industrial: approaching WHO guideline

            # Adjustment for energy intensity (combustion drives PM2.5)
            if energy_val is not None:
                # High energy use with coal -> higher PM2.5
                if energy_val > 4000 and co2pc_val and co2pc_val > 10:
                    pm25_est *= 1.3
                elif energy_val < 500:
                    pm25_est *= 0.85

            # Urban adjustment
            if urban_pct:
                pm25_est *= (0.7 + 0.006 * urban_pct)  # more urban = more PM2.5

            pm25_est = float(np.clip(pm25_est, 2.0, 120.0))

            # --- PM2.5 health cost (Burnett GEMM + VSL) ---
            # GEMM: relative risk of premature death from long-term PM2.5 exposure
            # RR(C) = exp(theta * ln(1 + C/alpha)) relative to counterfactual 2.4 ug/m3
            pm25_counterfactual = 2.4
            pm25_excess = max(0.0, pm25_est - pm25_counterfactual)

            if pm25_excess > 0:
                ln_term = np.log(1 + pm25_excess / self.GEMM_ALPHA)
                rr = np.exp(self.GEMM_BETA * ln_term)
            else:
                rr = 1.0

            attributable_fraction = (rr - 1) / rr
            crude_death_rate = 8.0 / 1000  # per person per year
            annual_deaths = pop * crude_death_rate
            pm25_deaths = annual_deaths * attributable_fraction

            vsl = 40 * gdppc   # value of statistical life
            mortality_cost = pm25_deaths * vsl

            # Morbidity cost: ~10x mortality in healthcare expenditure
            morbidity_cost = mortality_cost * 0.30

            # Work productivity loss (chronic illness)
            productivity_loss = gdp_val * 0.02 * attributable_fraction

            total_pm25_cost = mortality_cost + morbidity_cost + productivity_loss
            pm25_cost_pct_gdp = total_pm25_cost / gdp_val * 100

            who_compliance = pm25_est <= self.WHO_PM25_GUIDELINE
            interim_target = (
                "WHO_guideline" if pm25_est <= self.WHO_PM25_GUIDELINE
                else "IT4_10" if pm25_est <= self.WHO_PM25_SAFE
                else "IT3_15" if pm25_est <= self.WHO_PM25_IT3
                else "IT2_25" if pm25_est <= self.WHO_PM25_IT2
                else "IT1_35" if pm25_est <= self.WHO_PM25_IT1
                else "above_IT1"
            )

            pm25_health_cost = {
                "year": latest_yr,
                "estimated_pm25_ugm3": round(float(pm25_est), 1),
                "who_guideline_ugm3": self.WHO_PM25_GUIDELINE,
                "who_compliance": who_compliance,
                "interim_target": interim_target,
                "relative_risk": round(float(rr), 4),
                "attributable_fraction": round(float(attributable_fraction), 4),
                "pm25_attributable_deaths": round(float(pm25_deaths), 0),
                "mortality_cost_musd": round(mortality_cost / 1e6, 2),
                "morbidity_cost_musd": round(morbidity_cost / 1e6, 2),
                "productivity_loss_musd": round(productivity_loss / 1e6, 2),
                "total_pm25_cost_musd": round(total_pm25_cost / 1e6, 2),
                "pm25_cost_pct_gdp": round(pm25_cost_pct_gdp, 3),
            }

            # --- EKC analysis (Selden & Song) ---
            # Test cross-country: CO2pc ~ a + b*log(GDPpc) + c*[log(GDPpc)]^2
            co2pc_vals, gdppc_vals, iso_list = [], [], []
            for iso in set(co2pc_data.keys()) & set(gdppc_data.keys()):
                c_ts = co2pc_data[iso]
                g_ts = gdppc_data[iso]
                common = sorted(set(c_ts.keys()) & set(g_ts.keys()))
                if common:
                    yr = common[-1]
                    c_val = c_ts[yr]
                    g_val = g_ts[yr]
                    if c_val and g_val and g_val > 0:
                        co2pc_vals.append(float(c_val))
                        gdppc_vals.append(float(g_val))
                        iso_list.append(iso)

            ekc_result = None
            if len(co2pc_vals) >= 25:
                co2_arr = np.array(co2pc_vals)
                log_gdp = np.log(np.array(gdppc_vals))

                # Quadratic in log(GDPpc): EKC requires inverted-U (beta1>0, beta2<0)
                X_quad = np.column_stack([np.ones(len(log_gdp)), log_gdp, log_gdp ** 2])
                beta, _, _, _ = np.linalg.lstsq(X_quad, co2_arr, rcond=None)

                y_hat = X_quad @ beta
                ss_res = np.sum((co2_arr - y_hat) ** 2)
                ss_tot = np.sum((co2_arr - np.mean(co2_arr)) ** 2)
                r_sq = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

                # Turning point: d(CO2)/d(log_GDP) = 0 => log_GDP* = -b1/(2*b2)
                turning_point_log_gdp = None
                turning_point_gdp = None
                ekc_confirmed = False

                if beta[2] < 0 and beta[1] > 0:
                    ekc_confirmed = True
                    turning_point_log_gdp = -beta[1] / (2 * beta[2])
                    turning_point_gdp = np.exp(turning_point_log_gdp)

                # Target country position
                target_position = None
                if target and target in iso_list:
                    idx = iso_list.index(target)
                    predicted = y_hat[idx]
                    actual = co2_arr[idx]
                    residual = actual - predicted
                    target_position = {
                        "actual_co2pc": round(float(actual), 3),
                        "predicted_co2pc": round(float(predicted), 3),
                        "residual": round(float(residual), 3),
                        "above_ekc": bool(residual > 0),
                    }

                ekc_result = {
                    "n_countries": len(co2pc_vals),
                    "ekc_confirmed": ekc_confirmed,
                    "coefficients": {
                        "intercept": round(float(beta[0]), 4),
                        "log_gdp": round(float(beta[1]), 4),
                        "log_gdp_squared": round(float(beta[2]), 4),
                    },
                    "r_squared": round(float(r_sq), 4),
                    "turning_point_gdp_usd": (
                        round(float(turning_point_gdp), 0)
                        if turning_point_gdp else None
                    ),
                    "target_country": target_position,
                    "interpretation": (
                        "EKC holds: pollution peaks then declines with income"
                        if ekc_confirmed
                        else "EKC not confirmed in this sample"
                    ),
                }

            ekc_analysis = ekc_result or {
                "note": "insufficient cross-country data for EKC estimation"
            }

            # --- Clean air regulation CBA ---
            # Benefit: mortality and morbidity reduction from reaching interim target
            pm25_target = self.WHO_PM25_SAFE  # move to 10 ug/m3 target

            if pm25_est > pm25_target:
                pm25_reduction = pm25_est - pm25_target
                pm25_excess_target = max(0.0, pm25_target - pm25_counterfactual)

                if pm25_excess_target > 0:
                    rr_target = np.exp(
                        self.GEMM_BETA * np.log(1 + pm25_excess_target / self.GEMM_ALPHA)
                    )
                else:
                    rr_target = 1.0

                af_target = (rr_target - 1) / rr_target
                lives_saved = annual_deaths * (attributable_fraction - af_target)
                mortality_benefit = lives_saved * vsl
                morbidity_benefit = mortality_benefit * 0.30

                total_benefit = mortality_benefit + morbidity_benefit

                # Abatement cost: MAC-based, assume $5-50/tonne PM2.5 abated
                # PM2.5 from CO2 proxy: ~0.05 kg PM2.5 per kg CO2 (rough)
                if co2pc_val and pop:
                    total_co2_tonnes = co2pc_val * pop
                    pm25_abatement_cost_per_tonne = 20 * (gdppc / 10000) ** 0.4
                    # Convert CO2 to PM2.5: ~1:20 mass ratio proxy
                    pm25_tonnes_abated = total_co2_tonnes * pm25_reduction / pm25_est * 0.05
                    total_abatement_cost = pm25_tonnes_abated * pm25_abatement_cost_per_tonne
                else:
                    total_abatement_cost = total_benefit * 0.30  # assume 30% cost/benefit

                bcr = total_benefit / total_abatement_cost if total_abatement_cost > 0 else None

                clean_air_cba = {
                    "current_pm25_ugm3": round(float(pm25_est), 1),
                    "target_pm25_ugm3": pm25_target,
                    "pm25_reduction_ugm3": round(float(pm25_reduction), 1),
                    "lives_saved_annually": round(float(lives_saved), 0),
                    "annual_mortality_benefit_musd": round(mortality_benefit / 1e6, 2),
                    "annual_morbidity_benefit_musd": round(morbidity_benefit / 1e6, 2),
                    "total_annual_benefit_musd": round(total_benefit / 1e6, 2),
                    "estimated_abatement_cost_musd": round(total_abatement_cost / 1e6, 2),
                    "benefit_cost_ratio": round(float(bcr), 2) if bcr else None,
                    "regulation_justified": bool(bcr and bcr > 1),
                }
            else:
                clean_air_cba = {
                    "current_pm25_ugm3": round(float(pm25_est), 1),
                    "who_interim_target_ugm3": pm25_target,
                    "note": "country already meets WHO interim target 4",
                }

            # --- Transboundary pollution externalities ---
            # Gravity-decay model: pollution impact on neighbor = source * decay(distance)
            # Estimate from regional CO2 intensity spread
            co2_global_vals = []
            for iso in co2pc_data:
                c_ts = co2pc_data[iso]
                if c_ts:
                    co2_global_vals.append(c_ts[sorted(c_ts.keys())[-1]])

            co2_global_arr = np.array(co2_global_vals)
            target_co2pc = co2pc_ts.get(latest_yr) if co2pc_ts else None

            if target_co2pc is not None and len(co2_global_arr) > 5:
                # Countries within 2x and 0.5x of target CO2 intensity = neighbors
                neighbors_low = float(np.percentile(co2_global_arr, 10))
                neighbors_high = float(np.percentile(co2_global_arr, 90))

                # Approximate transboundary import: 20-30% of domestic pollution
                transboundary_import_pct = 25.0
                imported_pm25_contribution = pm25_est * transboundary_import_pct / 100

                # Cost of imported pollution
                imported_pm25_cost = total_pm25_cost * transboundary_import_pct / 100

                # Export externality: how much this country exports to neighbors
                if target_co2pc > float(np.median(co2_global_arr)):
                    export_externality_pct = 30.0
                else:
                    export_externality_pct = 15.0

                transboundary = {
                    "transboundary_pm25_import_pct": transboundary_import_pct,
                    "imported_pm25_ugm3": round(imported_pm25_contribution, 2),
                    "imported_pollution_cost_musd": round(imported_pm25_cost / 1e6, 2),
                    "export_externality_pct": export_externality_pct,
                    "net_polluter": bool(target_co2pc > float(np.median(co2_global_arr))),
                    "policy_implication": (
                        "regional_cooperation_urgently_needed"
                        if imported_pm25_contribution > 10
                        else "national_policy_sufficient"
                    ),
                }

        # --- Score ---
        score = 25.0

        if pm25_health_cost:
            pm25 = pm25_health_cost["estimated_pm25_ugm3"]
            if pm25 > 35:
                score += 35
            elif pm25 > 15:
                score += 20
            elif pm25 > 10:
                score += 10
            elif pm25 > 5:
                score += 5

        if pm25_health_cost:
            cost_pct = pm25_health_cost["pm25_cost_pct_gdp"]
            if cost_pct > 5:
                score += 25
            elif cost_pct > 2:
                score += 15
            elif cost_pct > 1:
                score += 8

        if ekc_analysis and isinstance(ekc_analysis, dict):
            if ekc_analysis.get("target_country", {}) and (
                ekc_analysis.get("target_country", {}).get("above_ekc")
            ):
                score += 10  # above EKC prediction = underperforming

        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "results": {
                "country_iso3": target,
                "pm25_health_cost": pm25_health_cost,
                "ekc_analysis": ekc_analysis,
                "clean_air_regulation_cba": clean_air_cba,
                "transboundary_externalities": transboundary,
            },
        }
