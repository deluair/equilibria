"""Climate damage functions: GDP impact per degree warming, tail risk, discount rates.

Estimates climate damage using the Burke-Hsiang-Miguel (2015) empirical damage
function, evaluates tail risk following Weitzman (2009), and analyzes the
Stern-Nordhaus discount rate debate.

Methodology:
    Burke-Hsiang-Miguel (BHM) damage function:
        ln(y_it) = f(T_it) + alpha_i + gamma_t + e_it

    where the temperature response f(T) is a quadratic with peak productivity
    around 13C. Damage per degree is country-specific based on baseline temperature.

    Weitzman tail risk: fat-tailed climate sensitivity distribution implies
    expected damages dominated by catastrophic outcomes.

    Discount rate debate:
        Nordhaus (descriptive): rho ~ 1.5%, eta ~ 2 => r ~ 5.5%
        Stern (prescriptive): rho ~ 0.1%, eta ~ 1 => r ~ 1.4%
        Ramsey rule: r = rho + eta * g

References:
    Burke, M., Hsiang, S.M. & Miguel, E. (2015). "Global non-linear effect of
        temperature on economic production." Nature, 527(7577), 235-239.
    Weitzman, M.L. (2009). "On modeling and interpreting the economics of
        catastrophic climate change." Review of Economics and Statistics, 91(1), 1-19.
    Stern, N. (2007). "The Economics of Climate Change: The Stern Review."
        Cambridge University Press.
    Nordhaus, W. (2007). "A review of the Stern Review on the economics of climate
        change." Journal of Economic Literature, 45(3), 686-702.
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class ClimateDamage(LayerBase):
    layer_id = "l9"
    name = "Climate Damage"

    # BHM quadratic temperature-growth function parameters
    # From Burke et al. (2015) preferred specification
    BHM_BETA1 = 0.0127   # linear temperature coefficient
    BHM_BETA2 = -0.000049  # quadratic temperature coefficient (note: very small)
    BHM_OPTIMAL_TEMP = 13.0  # approximate optimal temperature (C)

    # Warming scenarios (degrees C above preindustrial)
    WARMING_SCENARIOS = [1.5, 2.0, 3.0, 4.0, 5.0]

    async def compute(self, db, **kwargs) -> dict:
        """Estimate climate damage functions and GDP impact.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
            baseline_temp_c : float - country baseline mean temp (default from data)
            warming_by_2100 : float - expected warming (default 3.0)
        """
        country = kwargs.get("country_iso3", "BGD")
        baseline_temp = kwargs.get("baseline_temp_c")
        warming = kwargs.get("warming_by_2100", 3.0)

        # Fetch temperature and GDP data
        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN (
                  'NY.GDP.PCAP.KD', 'NY.GDP.MKTP.KD',
                  'AG.LND.TOTL.K2', 'SP.POP.TOTL'
              )
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient data"}

        data: dict[str, dict[str, float]] = {}
        for r in rows:
            sid = r["series_id"]
            yr = r["date"][:4] if isinstance(r["date"], str) else str(r["date"])
            data.setdefault(sid, {})[yr] = float(r["value"])

        gdp_pc_ts = data.get("NY.GDP.PCAP.KD", {})
        gdp_ts = data.get("NY.GDP.MKTP.KD", {})

        if not gdp_pc_ts:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "no GDP per capita data"}

        latest_year = sorted(gdp_pc_ts.keys())[-1]
        gdp_pc = gdp_pc_ts[latest_year]
        gdp_total = gdp_ts.get(latest_year, gdp_pc * 170e6)  # fallback

        # Use provided baseline temp or approximate from latitude
        if baseline_temp is None:
            # Rough approximation for tropical/subtropical countries
            baseline_temp = self._approximate_baseline_temp(country)

        # 1. Burke-Hsiang-Miguel damage estimates
        bhm_damages = self._bhm_damage(
            baseline_temp=baseline_temp,
            gdp_pc=gdp_pc,
            gdp_total=gdp_total,
        )

        # 2. Weitzman tail risk analysis
        tail_risk = self._weitzman_tail_risk(warming_expected=warming)

        # 3. Discount rate debate (Ramsey rule variants)
        discount_analysis = self._discount_rate_analysis(
            gdp_growth_rate=self._estimate_growth_rate(gdp_pc_ts),
        )

        # 4. Country vulnerability assessment
        vulnerability = self._assess_vulnerability(
            baseline_temp=baseline_temp,
            gdp_pc=gdp_pc,
            warming=warming,
        )

        # Score: hot + poor + high warming scenario = high stress
        temp_vulnerability = max(0, (baseline_temp - self.BHM_OPTIMAL_TEMP) / 15) * 40
        income_vulnerability = max(0, 1 - np.log(gdp_pc) / np.log(50000)) * 30
        warming_score = min(30, warming / 5.0 * 30)
        score = float(np.clip(temp_vulnerability + income_vulnerability + warming_score, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "baseline_temp_c": round(baseline_temp, 1),
            "gdp_pc_usd": round(gdp_pc, 2),
            "bhm_damages": bhm_damages,
            "tail_risk": tail_risk,
            "discount_rate_analysis": discount_analysis,
            "vulnerability": vulnerability,
        }

    def _bhm_damage(
        self, baseline_temp: float, gdp_pc: float, gdp_total: float
    ) -> dict:
        """Burke-Hsiang-Miguel empirical damage function.

        Growth effect of temperature: g(T) = beta1*T + beta2*T^2.
        Marginal effect: dg/dT = beta1 + 2*beta2*T.
        For countries above optimal temp, warming reduces growth.
        """
        b1 = self.BHM_BETA1
        b2 = self.BHM_BETA2

        # Growth effect at baseline
        growth_at_baseline = b1 * baseline_temp + b2 * baseline_temp ** 2
        marginal_at_baseline = b1 + 2 * b2 * baseline_temp

        damages = {}
        for delta in self.WARMING_SCENARIOS:
            new_temp = baseline_temp + delta
            growth_at_new = b1 * new_temp + b2 * new_temp ** 2
            growth_loss = growth_at_new - growth_at_baseline

            # Cumulative GDP loss over 80 years (to 2100)
            # If growth rate falls by dg per year, cumulative loss compounds
            years = 80
            baseline_growth = 0.03  # assume 3% baseline growth
            gdp_no_cc = gdp_total * (1 + baseline_growth) ** years
            gdp_with_cc = gdp_total * (1 + baseline_growth + growth_loss) ** years
            gdp_loss_pct = (1 - gdp_with_cc / gdp_no_cc) * 100 if gdp_no_cc > 0 else 0

            damages[f"+{delta}C"] = {
                "annual_growth_loss_pp": round(float(growth_loss) * 100, 3),
                "cumulative_gdp_loss_pct_by_2100": round(float(gdp_loss_pct), 2),
                "gdp_loss_usd": round(float(gdp_no_cc - gdp_with_cc), 0),
            }

        return {
            "growth_at_baseline": round(float(growth_at_baseline), 6),
            "marginal_effect_at_baseline": round(float(marginal_at_baseline), 6),
            "above_optimal": baseline_temp > self.BHM_OPTIMAL_TEMP,
            "damages_by_scenario": damages,
        }

    @staticmethod
    def _weitzman_tail_risk(warming_expected: float) -> dict:
        """Weitzman (2009) fat-tailed climate sensitivity and catastrophic risk.

        Climate sensitivity is not normally distributed. Fat tails mean
        the probability of extreme warming (>6C) is non-negligible,
        and expected damages can be dominated by catastrophic states.
        """
        # Climate sensitivity distribution: log-normal approximation
        # Median ~3C, but fat right tail
        mu_cs = np.log(3.0)
        sigma_cs = 0.5  # gives meaningful right tail

        # Probability of exceeding various warming thresholds
        thresholds = [2.0, 3.0, 4.0, 5.0, 6.0, 8.0]
        exceedance_probs = {}
        for threshold in thresholds:
            prob = 1.0 - stats.lognorm.cdf(threshold, s=sigma_cs, scale=np.exp(mu_cs))
            exceedance_probs[f">{threshold}C"] = round(float(prob) * 100, 2)

        # Expected damage under fat-tailed distribution
        # Damage function: D(T) = a * T^2 (quadratic) vs D(T) = a * exp(b*T) (exponential)
        a_quad = 0.00236  # Nordhaus DICE
        a_exp = 0.001
        b_exp = 0.5

        n_samples = 10000
        cs_samples = stats.lognorm.rvs(s=sigma_cs, scale=np.exp(mu_cs), size=n_samples)

        # Quadratic damage expectation
        quad_damages = a_quad * cs_samples ** 2 * 100  # % GDP
        # Exponential damage expectation (Weitzman catastrophic)
        exp_damages = a_exp * np.exp(b_exp * cs_samples) * 100

        return {
            "climate_sensitivity_median": 3.0,
            "exceedance_probabilities_pct": exceedance_probs,
            "expected_damage_quadratic_pct_gdp": round(float(np.mean(quad_damages)), 2),
            "expected_damage_exponential_pct_gdp": round(float(np.mean(exp_damages)), 2),
            "p95_damage_quadratic": round(float(np.percentile(quad_damages, 95)), 2),
            "p95_damage_exponential": round(float(np.percentile(exp_damages, 95)), 2),
            "tail_risk_ratio": round(
                float(np.mean(exp_damages)) / float(np.mean(quad_damages)), 2
            ) if np.mean(quad_damages) > 0 else None,
        }

    @staticmethod
    def _discount_rate_analysis(gdp_growth_rate: float) -> dict:
        """Stern vs Nordhaus discount rate debate via Ramsey rule.

        r = rho + eta * g
        where rho = pure time preference, eta = elasticity of marginal utility,
        g = per capita consumption growth rate.
        """
        g = gdp_growth_rate

        frameworks = {
            "nordhaus": {"rho": 0.015, "eta": 2.0, "description": "descriptive (market-based)"},
            "stern": {"rho": 0.001, "eta": 1.0, "description": "prescriptive (ethical)"},
            "weitzman": {"rho": 0.005, "eta": 1.5, "description": "compromise"},
        }

        results = {}
        for name, params in frameworks.items():
            r = params["rho"] + params["eta"] * g
            results[name] = {
                "rho": params["rho"],
                "eta": params["eta"],
                "growth_rate": round(g, 4),
                "discount_rate": round(r, 4),
                "description": params["description"],
            }

        # SCC sensitivity to discount rate
        # Higher discount rate -> lower SCC (future damages discounted more)
        scc_ratio_stern_nordhaus = (
            (1 + results["nordhaus"]["discount_rate"]) /
            (1 + results["stern"]["discount_rate"])
        ) ** 50  # 50-year horizon factor
        results["scc_sensitivity"] = {
            "stern_nordhaus_ratio_50yr": round(float(scc_ratio_stern_nordhaus), 2),
            "implication": "Stern SCC roughly {}x higher than Nordhaus SCC".format(
                round(float(scc_ratio_stern_nordhaus), 1)
            ),
        }

        return results

    @staticmethod
    def _assess_vulnerability(
        baseline_temp: float, gdp_pc: float, warming: float
    ) -> dict:
        """Composite climate vulnerability assessment."""
        # Temperature exposure (how far above optimal)
        temp_exposure = max(0, baseline_temp + warming - 13.0) / 20.0
        # Adaptive capacity (income proxy)
        adaptive_capacity = min(1.0, np.log(max(gdp_pc, 1)) / np.log(50000))
        # Vulnerability = exposure * (1 - adaptive capacity)
        vulnerability_index = temp_exposure * (1 - adaptive_capacity)

        return {
            "temperature_exposure": round(float(temp_exposure), 4),
            "adaptive_capacity": round(float(adaptive_capacity), 4),
            "vulnerability_index": round(float(vulnerability_index), 4),
            "category": "low" if vulnerability_index < 0.2 else
                       "moderate" if vulnerability_index < 0.4 else
                       "high" if vulnerability_index < 0.6 else "very_high",
        }

    @staticmethod
    def _estimate_growth_rate(gdp_pc_ts: dict[str, float]) -> float:
        """Estimate average GDP per capita growth rate from time series."""
        years = sorted(gdp_pc_ts.keys())
        if len(years) < 2:
            return 0.02  # default assumption
        vals = np.array([gdp_pc_ts[y] for y in years])
        growth_rates = np.diff(vals) / vals[:-1]
        return float(np.mean(growth_rates))

    @staticmethod
    def _approximate_baseline_temp(country_iso3: str) -> float:
        """Rough baseline temperature approximation by country.

        Tropical countries ~25-28C, temperate ~10-15C, cold ~0-5C.
        """
        tropical = {"BGD", "IND", "IDN", "THA", "VNM", "PHL", "NGA", "GHA",
                     "KEN", "TZA", "BRA", "COL", "PER", "MEX", "EGY", "PAK",
                     "MMR", "KHM", "LAO", "LKA", "NPL", "SDN", "ETH", "MOZ"}
        temperate = {"USA", "CHN", "JPN", "KOR", "FRA", "DEU", "GBR", "ITA",
                     "ESP", "TUR", "POL", "ARG", "CHL", "AUS", "ZAF", "IRN"}
        cold = {"RUS", "CAN", "NOR", "SWE", "FIN", "ISL", "DNK"}

        if country_iso3 in tropical:
            return 26.0
        elif country_iso3 in temperate:
            return 12.0
        elif country_iso3 in cold:
            return 3.0
        return 15.0  # default moderate
