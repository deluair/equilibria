"""Renewable energy transition: learning curves, LCOE, grid integration, stranded assets.

Estimates learning rates for solar and wind technologies, computes levelized cost
of energy (LCOE), evaluates grid integration costs of variable renewables, and
estimates stranded fossil fuel asset risk.

Methodology:
    Learning curve (Wright's Law):
        C(x) = C_0 * x^(-alpha)
        Learning rate = 1 - 2^(-alpha)

    where C is cost per unit, x is cumulative production, alpha is the learning
    exponent. Solar PV has ~20-24% learning rate, onshore wind ~12-15%.

    Levelized Cost of Energy:
        LCOE = (sum_t [I_t + M_t + F_t] / (1+r)^t) / (sum_t E_t / (1+r)^t)

    where I = investment, M = O&M, F = fuel, E = electricity output, r = discount rate.

    Stranded assets: NPV of fossil fuel reserves that become unburnable
    under carbon budget constraints (McGlade & Ekins 2015).

References:
    Way, R. et al. (2022). "Empirically grounded technology forecasts and the energy
        transition." Joule, 6(9), 2057-2082.
    IRENA (2023). "Renewable Power Generation Costs in 2022."
    McGlade, C. & Ekins, P. (2015). "The geographical distribution of fossil fuels
        unused when limiting global warming to 2C." Nature, 517(7533), 187-190.
    Joskow, P. (2011). "Comparing the costs of intermittent and dispatchable
        electricity generating technologies." AER, 101(3), 238-241.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class RenewableTransition(LayerBase):
    layer_id = "l9"
    name = "Renewable Transition"

    # Benchmark learning rates (Way et al. 2022, IRENA)
    BENCHMARK_LEARNING_RATES = {
        "solar_pv": 0.24,     # 24% cost reduction per doubling
        "onshore_wind": 0.15,
        "offshore_wind": 0.10,
        "battery_li_ion": 0.18,
    }

    # Current approximate LCOE benchmarks (USD/MWh, IRENA 2023)
    LCOE_BENCHMARKS = {
        "solar_pv": 49,
        "onshore_wind": 33,
        "offshore_wind": 75,
        "coal": 65,
        "gas_ccgt": 55,
        "nuclear": 70,
    }

    async def compute(self, db, **kwargs) -> dict:
        """Compute renewable transition analysis.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
            discount_rate : float - for LCOE calculation (default 0.07)
            carbon_budget_gtco2 : float - remaining carbon budget (default 500)
        """
        country = kwargs.get("country_iso3", "BGD")
        discount_rate = kwargs.get("discount_rate", 0.07)
        carbon_budget = kwargs.get("carbon_budget_gtco2", 500.0)

        # Fetch energy data
        energy_series = [
            "EG.ELC.RNEW.ZS",     # Renewable electricity (% total)
            "EG.FEC.RNEW.ZS",     # Renewable energy consumption (% total)
            "EG.USE.PCAP.KG.OE",  # Energy use per capita (kg oil equiv)
            "EN.ATM.CO2E.KT",     # CO2 emissions (kt)
            "NY.GDP.MKTP.KD",     # GDP
            "EG.ELC.COAL.ZS",     # Electricity from coal (%)
            "EG.ELC.NGAS.ZS",     # Electricity from natural gas (%)
        ]
        placeholders = ",".join(["?" for _ in energy_series])

        rows = await db.fetch_all(
            f"""
            SELECT dp.date, dp.value, ds.series_id
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ({placeholders})
              AND dp.value IS NOT NULL
            ORDER BY dp.date
            """,
            (country, *energy_series),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient energy data"}

        data: dict[str, dict[str, float]] = {}
        for r in rows:
            sid = r["series_id"]
            yr = r["date"][:4] if isinstance(r["date"], str) else str(r["date"])
            data.setdefault(sid, {})[yr] = float(r["value"])

        re_share_ts = data.get("EG.ELC.RNEW.ZS", {})
        co2_ts = data.get("EN.ATM.CO2E.KT", {})
        gdp_ts = data.get("NY.GDP.MKTP.KD", {})

        # 1. Learning curves for solar/wind
        learning_curves = self._compute_learning_curves()

        # 2. LCOE comparison
        lcoe = self._compute_lcoe(discount_rate=discount_rate)

        # 3. Renewable share trajectory and projection
        re_trajectory = self._analyze_re_trajectory(re_share_ts)

        # 4. Stranded asset estimation
        stranded = self._estimate_stranded_assets(
            co2_ts=co2_ts,
            gdp_ts=gdp_ts,
            fossil_share_ts={
                "coal": data.get("EG.ELC.COAL.ZS", {}),
                "gas": data.get("EG.ELC.NGAS.ZS", {}),
            },
            carbon_budget_gtco2=carbon_budget,
        )

        # 5. Grid integration cost estimate
        grid_integration = self._estimate_grid_integration(
            re_share=re_trajectory.get("latest_re_share_pct", 0),
        )

        # Score: higher fossil dependence + slower transition = higher stress
        re_share = re_trajectory.get("latest_re_share_pct", 0)
        re_trend = re_trajectory.get("trend_pct_per_year", 0)

        fossil_score = max(0, min(50, (100 - re_share) * 0.5))
        transition_speed_score = max(0, min(30, 30 - re_trend * 10))
        stranded_score = min(20, stranded.get("stranded_risk_pct_gdp", 0) * 5)

        score = float(np.clip(fossil_score + transition_speed_score + stranded_score, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "learning_curves": learning_curves,
            "lcoe_comparison": lcoe,
            "renewable_trajectory": re_trajectory,
            "stranded_assets": stranded,
            "grid_integration": grid_integration,
        }

    def _compute_learning_curves(self) -> dict:
        """Compute technology learning curves using Wright's Law.

        C(x) = C_0 * x^(-alpha), learning rate = 1 - 2^(-alpha)
        """
        results = {}
        for tech, lr in self.BENCHMARK_LEARNING_RATES.items():
            alpha = -np.log2(1 - lr)

            # Project costs at future cumulative doublings
            doublings = [1, 2, 3, 4, 5]
            cost_indices = [(2 ** d) ** (-alpha) for d in doublings]

            results[tech] = {
                "learning_rate": round(lr, 4),
                "learning_exponent_alpha": round(float(alpha), 4),
                "cost_at_doublings": {
                    f"{d}x": round(float(ci) * 100, 1) for d, ci in zip(doublings, cost_indices)
                },
                "halving_doublings": round(1 / alpha, 2) if alpha > 0 else None,
            }
        return results

    def _compute_lcoe(self, discount_rate: float) -> dict:
        """Compare levelized cost of energy across technologies."""
        results = {}

        for tech, lcoe_base in self.LCOE_BENCHMARKS.items():
            # Simple LCOE with capacity factor and lifetime adjustments
            if tech in ("solar_pv", "onshore_wind", "offshore_wind"):
                capacity_factors = {"solar_pv": 0.18, "onshore_wind": 0.30, "offshore_wind": 0.40}
                cf = capacity_factors.get(tech, 0.25)
                lifetime = 25
            else:
                cf = 0.85 if tech == "nuclear" else 0.60
                lifetime = 40 if tech == "nuclear" else 30

            # Annuity factor
            annuity = (discount_rate * (1 + discount_rate) ** lifetime) / (
                (1 + discount_rate) ** lifetime - 1
            )

            results[tech] = {
                "lcoe_usd_per_mwh": round(lcoe_base, 1),
                "capacity_factor": cf,
                "lifetime_years": lifetime,
                "annualized_cost_factor": round(float(annuity), 4),
            }

        # Find crossover point
        cheapest = min(results.items(), key=lambda x: x[1]["lcoe_usd_per_mwh"])
        return {
            "technologies": results,
            "cheapest": cheapest[0],
            "cheapest_lcoe": cheapest[1]["lcoe_usd_per_mwh"],
            "discount_rate": discount_rate,
        }

    @staticmethod
    def _analyze_re_trajectory(re_share_ts: dict[str, float]) -> dict:
        """Analyze renewable energy share trajectory and project forward."""
        if not re_share_ts:
            return {"error": "no renewable share data"}

        years = sorted(re_share_ts.keys())
        vals = np.array([re_share_ts[y] for y in years])
        yrs = np.array([int(y) for y in years])

        latest = float(vals[-1])

        # Linear trend
        if len(vals) >= 3:
            trend = np.polyfit(yrs - yrs[0], vals, 1)
            trend_slope = float(trend[0])
        else:
            trend_slope = 0.0

        # Project to 50% and 100% renewable
        if trend_slope > 0:
            years_to_50 = max(0, (50 - latest) / trend_slope)
            years_to_100 = max(0, (100 - latest) / trend_slope)
            year_reach_50 = int(yrs[-1] + years_to_50)
            year_reach_100 = int(yrs[-1] + years_to_100)
        else:
            year_reach_50 = None
            year_reach_100 = None

        return {
            "latest_re_share_pct": round(latest, 2),
            "latest_year": years[-1],
            "trend_pct_per_year": round(trend_slope, 3),
            "n_years": len(years),
            "projected_year_50pct": year_reach_50,
            "projected_year_100pct": year_reach_100,
        }

    @staticmethod
    def _estimate_stranded_assets(
        co2_ts: dict[str, float],
        gdp_ts: dict[str, float],
        fossil_share_ts: dict[str, dict[str, float]],
        carbon_budget_gtco2: float,
    ) -> dict:
        """Estimate stranded fossil fuel asset risk.

        Following McGlade & Ekins (2015): fraction of reserves that
        become unburnable under a carbon budget constraint.
        """
        common = sorted(set(co2_ts.keys()) & set(gdp_ts.keys()))
        if not common:
            return {"error": "insufficient data"}

        latest = common[-1]
        co2_kt = co2_ts[latest]
        gdp = gdp_ts[latest]
        co2_gt = co2_kt / 1e6  # kt to Gt

        # Years of current emissions before budget exhausted
        if co2_gt > 0:
            years_remaining = carbon_budget_gtco2 / co2_gt
        else:
            years_remaining = float("inf")

        # Fossil asset value proxy: energy rents as share of GDP
        coal_share = 0
        gas_share = 0
        for fuel, ts in fossil_share_ts.items():
            if latest in ts:
                if fuel == "coal":
                    coal_share = ts[latest]
                elif fuel == "gas":
                    gas_share = ts[latest]

        fossil_electricity_share = coal_share + gas_share
        # Approximate stranded asset value: fossil share * GDP * remaining lifetime
        asset_lifetime = 30  # typical power plant lifetime
        stranded_fraction = max(0, 1.0 - years_remaining / asset_lifetime)
        stranded_value = gdp * fossil_electricity_share / 100.0 * stranded_fraction * 5

        return {
            "annual_co2_gt": round(co2_gt, 4),
            "carbon_budget_gt": carbon_budget_gtco2,
            "years_remaining_at_current_rate": round(years_remaining, 1),
            "fossil_electricity_share_pct": round(fossil_electricity_share, 2),
            "stranded_fraction": round(stranded_fraction, 4),
            "stranded_value_usd": round(stranded_value, 0),
            "stranded_risk_pct_gdp": round(stranded_value / gdp * 100, 4) if gdp > 0 else 0,
        }

    @staticmethod
    def _estimate_grid_integration(re_share: float) -> dict:
        """Estimate grid integration costs of variable renewables.

        Integration costs rise nonlinearly with renewable penetration
        due to balancing, profile, and grid reinforcement costs.

        Based on Hirth (2013) and IEA (2014) system integration studies.
        """
        # Integration cost curve (USD/MWh) as function of RE share
        # Piecewise: low at <20%, rising steeply at 30-50%, high plateau at >50%
        if re_share < 10:
            integration_cost = 2.0
        elif re_share < 20:
            integration_cost = 2.0 + (re_share - 10) * 0.3
        elif re_share < 30:
            integration_cost = 5.0 + (re_share - 20) * 0.5
        elif re_share < 50:
            integration_cost = 10.0 + (re_share - 30) * 0.8
        else:
            integration_cost = 26.0 + (re_share - 50) * 0.4

        # Breakdown by component (approximate shares)
        balancing = integration_cost * 0.35
        profile = integration_cost * 0.40
        grid_reinforcement = integration_cost * 0.25

        return {
            "re_share_pct": round(re_share, 2),
            "total_integration_cost_usd_mwh": round(integration_cost, 2),
            "balancing_cost": round(balancing, 2),
            "profile_cost": round(profile, 2),
            "grid_reinforcement_cost": round(grid_reinforcement, 2),
            "phase": "low" if re_share < 15 else
                    "moderate" if re_share < 30 else
                    "challenging" if re_share < 50 else "systemic",
        }
