"""Carbon pricing: tax incidence, ETS dynamics, social cost of carbon, border adjustment.

Estimates the economic impact of carbon pricing instruments using the Nordhaus DICE
framework for social cost of carbon, analyzes ETS price dynamics, computes carbon
tax incidence across income deciles, and evaluates border carbon adjustment (CBAM)
trade effects.

Methodology:
    Social cost of carbon via simplified DICE (Nordhaus 2017):
        SCC = sum_{t=0}^{T} [dC(T_t)/dE_0] / [(1+rho)^t]

    where C is climate damage, T is temperature, E is emissions, rho is discount rate.

    Carbon tax incidence: distributional burden across income deciles using
    household expenditure shares on carbon-intensive goods.

    ETS price dynamics: permit price as function of cap stringency, banking
    provisions, and abatement cost curve (MAC).

    Border carbon adjustment: embodied carbon in trade * carbon price differential.

References:
    Nordhaus, W. (2017). "Revisiting the social cost of carbon." PNAS, 114(7), 1518-1523.
    Metcalf, G. & Stock, J. (2020). "Measuring the macroeconomic impact of carbon taxes."
        AER Papers & Proceedings, 110, 101-106.
    Coase, R. (1960). "The problem of social cost." Journal of Law and Economics, 3, 1-44.
"""

from __future__ import annotations

import numpy as np
from scipy import optimize

from app.layers.base import LayerBase


class CarbonPricing(LayerBase):
    layer_id = "l9"
    name = "Carbon Pricing"

    # Nordhaus DICE simplified parameters
    DEFAULT_DISCOUNT_RATE = 0.015  # Nordhaus baseline
    CLIMATE_SENSITIVITY = 3.0  # degrees C per doubling CO2
    DAMAGE_COEFFICIENT = 0.00236  # quadratic damage coefficient (Nordhaus 2017)
    CARBON_CYCLE_DECAY = 0.0083  # fraction of CO2 removed per year

    async def compute(self, db, **kwargs) -> dict:
        """Compute carbon pricing analysis.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
            discount_rate : float - social discount rate (default 0.015)
            carbon_price_usd : float - current/proposed carbon price per tCO2
            horizon_years : int - projection horizon (default 100)
        """
        country = kwargs.get("country_iso3", "BGD")
        rho = kwargs.get("discount_rate", self.DEFAULT_DISCOUNT_RATE)
        carbon_price = kwargs.get("carbon_price_usd")
        horizon = kwargs.get("horizon_years", 100)

        # Fetch emissions and GDP data
        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN (
                  'EN.ATM.CO2E.KT', 'EN.ATM.CO2E.PC',
                  'NY.GDP.MKTP.KD', 'NY.GDP.PCAP.KD'
              )
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 10:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient emissions/GDP data"}

        # Parse into time series
        series: dict[str, dict[str, float]] = {}
        for r in rows:
            sid = r["series_id"]
            yr = r["date"][:4] if isinstance(r["date"], str) else str(r["date"])
            series.setdefault(sid, {})[yr] = float(r["value"])

        co2_kt = series.get("EN.ATM.CO2E.KT", {})
        gdp = series.get("NY.GDP.MKTP.KD", {})
        co2_pc = series.get("EN.ATM.CO2E.PC", {})

        common_years = sorted(set(co2_kt.keys()) & set(gdp.keys()))
        if len(common_years) < 5:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient matched emissions-GDP data"}

        emissions = np.array([co2_kt[y] for y in common_years])
        gdp_vals = np.array([gdp[y] for y in common_years])
        years = np.array([int(y) for y in common_years])

        # Carbon intensity trend
        carbon_intensity = emissions / gdp_vals * 1e6  # tCO2 per million USD
        ci_trend = np.polyfit(years - years[0], carbon_intensity, 1)

        # Social cost of carbon (simplified DICE)
        scc = self._estimate_scc(
            emissions_kt=float(emissions[-1]),
            gdp_usd=float(gdp_vals[-1]),
            discount_rate=rho,
            horizon=horizon,
        )

        # ETS price dynamics (MAC-based equilibrium)
        mac_params = self._estimate_mac(emissions, gdp_vals)

        # Carbon tax incidence (approximate via expenditure shares)
        incidence = self._compute_tax_incidence(
            carbon_price=carbon_price or scc["scc_usd_per_tco2"],
            emissions_kt=float(emissions[-1]),
            gdp_usd=float(gdp_vals[-1]),
        )

        # Border carbon adjustment impact
        bca = self._border_carbon_adjustment(
            carbon_intensity_domestic=float(carbon_intensity[-1]),
            scc=scc["scc_usd_per_tco2"],
        )

        # Score: higher carbon intensity + lower pricing = higher stress
        # Countries with high emissions per GDP and no carbon price are at risk
        ci_percentile = float(np.clip(carbon_intensity[-1] / 1000.0 * 50, 0, 50))
        pricing_gap = 0.0
        if carbon_price is not None:
            pricing_gap = max(0, scc["scc_usd_per_tco2"] - carbon_price)
            pricing_gap_score = min(50, pricing_gap / scc["scc_usd_per_tco2"] * 50)
        else:
            pricing_gap_score = 40  # no carbon price at all
        score = float(np.clip(ci_percentile + pricing_gap_score, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "n_years": len(common_years),
            "carbon_intensity": {
                "latest_tco2_per_musd": round(float(carbon_intensity[-1]), 2),
                "trend_slope": round(float(ci_trend[0]), 4),
                "trend_direction": "declining" if ci_trend[0] < 0 else "increasing",
            },
            "social_cost_of_carbon": scc,
            "mac_curve": mac_params,
            "tax_incidence": incidence,
            "border_carbon_adjustment": bca,
            "current_carbon_price": carbon_price,
        }

    def _estimate_scc(
        self,
        emissions_kt: float,
        gdp_usd: float,
        discount_rate: float,
        horizon: int,
    ) -> dict:
        """Simplified Nordhaus DICE social cost of carbon estimate.

        Uses quadratic damage function: D(T) = a2 * T^2, where T is temperature
        above preindustrial. Marginal damage of one additional tonne CO2 is
        discounted over the horizon.
        """
        a2 = self.DAMAGE_COEFFICIENT
        cs = self.CLIMATE_SENSITIVITY
        decay = self.CARBON_CYCLE_DECAY

        # Approximate temperature response to marginal emission pulse (1 GtCO2)
        # Using simplified carbon cycle + climate sensitivity
        pulse_gtco2 = 1e-6  # 1 tonne = 1e-6 Gt
        co2_ppm_per_gt = 0.128  # approx ppm per GtC (= GtCO2 / 3.667)
        baseline_co2_ppm = 420.0
        baseline_temp = 1.2  # current warming above preindustrial

        scc_sum = 0.0
        remaining_pulse = pulse_gtco2
        for t in range(1, horizon + 1):
            remaining_pulse *= (1 - decay)
            delta_ppm = remaining_pulse * co2_ppm_per_gt / 3.667
            delta_temp = cs * np.log2(1 + delta_ppm / baseline_co2_ppm)
            temp_t = baseline_temp + delta_temp
            # Marginal damage: d(D)/d(E) = 2 * a2 * T * (dT/dE) * GDP
            marginal_damage = 2 * a2 * temp_t * delta_temp * gdp_usd
            scc_sum += marginal_damage / (1 + discount_rate) ** t

        # Sensitivity: Stern discount rate (0.1%)
        scc_stern = 0.0
        remaining_pulse = pulse_gtco2
        for t in range(1, horizon + 1):
            remaining_pulse *= (1 - decay)
            delta_ppm = remaining_pulse * co2_ppm_per_gt / 3.667
            delta_temp = cs * np.log2(1 + delta_ppm / baseline_co2_ppm)
            temp_t = baseline_temp + delta_temp
            marginal_damage = 2 * a2 * temp_t * delta_temp * gdp_usd
            scc_stern += marginal_damage / (1 + 0.001) ** t

        return {
            "scc_usd_per_tco2": round(scc_sum, 2),
            "scc_stern_usd_per_tco2": round(scc_stern, 2),
            "discount_rate": discount_rate,
            "horizon_years": horizon,
            "damage_coefficient": a2,
        }

    @staticmethod
    def _estimate_mac(emissions: np.ndarray, gdp: np.ndarray) -> dict:
        """Estimate marginal abatement cost curve parameters.

        MAC(q) = c * exp(alpha * q) where q is fraction of emissions abated.
        Calibrated from observed carbon intensity improvements.
        """
        intensity = emissions / gdp
        if len(intensity) < 3:
            return {"error": "insufficient data for MAC estimation"}

        # Implied abatement over time (relative to first year)
        abatement_frac = 1.0 - intensity / intensity[0]
        abatement_frac = np.clip(abatement_frac, 0, 0.99)

        # Fit exponential MAC: ln(implied_cost) ~ alpha * abatement
        # Use GDP growth as proxy for abatement cost
        gdp_growth = np.diff(gdp) / gdp[:-1]
        avg_cost_proxy = float(np.mean(np.abs(gdp_growth)))

        # Simple calibration: MAC at current abatement level
        current_abatement = float(abatement_frac[-1])
        alpha = 3.0  # standard MAC curvature
        c = avg_cost_proxy * 100  # base cost in USD

        mac_at_10pct = c * np.exp(alpha * 0.10)
        mac_at_25pct = c * np.exp(alpha * 0.25)
        mac_at_50pct = c * np.exp(alpha * 0.50)

        return {
            "base_cost_c": round(c, 2),
            "curvature_alpha": alpha,
            "current_abatement_frac": round(current_abatement, 4),
            "mac_at_10pct_abatement": round(float(mac_at_10pct), 2),
            "mac_at_25pct_abatement": round(float(mac_at_25pct), 2),
            "mac_at_50pct_abatement": round(float(mac_at_50pct), 2),
        }

    @staticmethod
    def _compute_tax_incidence(
        carbon_price: float, emissions_kt: float, gdp_usd: float
    ) -> dict:
        """Approximate carbon tax incidence across income deciles.

        Lower-income households spend larger shares on energy and transport,
        making carbon taxes regressive absent redistribution.
        """
        # Approximate energy expenditure shares by income decile
        # Based on Metcalf (2019) distributional analysis
        decile_energy_shares = np.array([
            0.21, 0.17, 0.14, 0.12, 0.11, 0.10, 0.09, 0.08, 0.07, 0.05
        ])

        total_revenue = carbon_price * emissions_kt * 1000  # USD (kt -> tonnes)
        revenue_pct_gdp = total_revenue / gdp_usd * 100

        # Burden per decile (assuming equal GDP shares adjusted by energy share)
        decile_gdp_share = np.array([0.02, 0.04, 0.05, 0.07, 0.08, 0.10,
                                     0.12, 0.14, 0.17, 0.21])
        decile_burden_pct = (
            decile_energy_shares * carbon_price * emissions_kt * 1000
            / (decile_gdp_share * gdp_usd) * 100
        )

        # Suits index (progressivity measure, -1 to +1, negative = regressive)
        cumulative_income = np.cumsum(decile_gdp_share)
        cumulative_burden = np.cumsum(decile_energy_shares * decile_gdp_share)
        cumulative_burden /= cumulative_burden[-1]
        suits_index = 1.0 - 2.0 * float(np.trapz(cumulative_burden, cumulative_income))

        return {
            "carbon_price_usd": round(carbon_price, 2),
            "total_revenue_musd": round(total_revenue / 1e6, 2),
            "revenue_pct_gdp": round(revenue_pct_gdp, 4),
            "burden_by_decile_pct": [round(float(x), 3) for x in decile_burden_pct],
            "suits_index": round(suits_index, 4),
            "regressivity": "regressive" if suits_index < -0.05 else
                           "proportional" if suits_index < 0.05 else "progressive",
        }

    @staticmethod
    def _border_carbon_adjustment(
        carbon_intensity_domestic: float, scc: float
    ) -> dict:
        """Estimate border carbon adjustment impact.

        BCA tariff = (embodied carbon - domestic benchmark) * carbon price.
        """
        # Approximate global average carbon intensity
        global_avg_ci = 300.0  # tCO2 per million USD (world average)

        ci_gap = carbon_intensity_domestic - global_avg_ci
        bca_tariff_equiv = abs(ci_gap) * scc / 1e6 * 100  # as pct of trade value

        return {
            "domestic_ci_tco2_per_musd": round(carbon_intensity_domestic, 2),
            "global_avg_ci": global_avg_ci,
            "ci_gap": round(ci_gap, 2),
            "bca_tariff_equiv_pct": round(float(bca_tariff_equiv), 4),
            "direction": "exports_face_bca" if ci_gap > 0 else "imports_face_bca",
        }
