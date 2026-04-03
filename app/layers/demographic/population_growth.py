"""Population growth: Malthusian trap, unified growth theory, momentum.

Tests the Malthusian hypothesis that population growth outstrips resource
growth, leading to subsistence equilibrium. Implements Galor's (2005) unified
growth theory framework that explains the transition from Malthusian stagnation
through post-Malthusian growth to modern sustained growth.

Malthusian trap test:
    If d(GDP_pc)/d(population) < 0 and population growth responds positively
    to income above subsistence, the economy is in a Malthusian regime.

Unified growth theory (Galor 2005, 2011):
    - Malthusian epoch: technology -> population, constant living standards
    - Post-Malthusian: technology faster than population, rising standards
    - Modern growth: demographic transition, human capital investment, sustained

Demographic-economic model (Solow + population):
    Y = K^alpha * (A*L)^(1-alpha)
    Capital dilution: higher n requires higher savings to maintain k*

Population momentum: even after TFR reaches replacement, population continues
to grow because of age structure (large cohorts in reproductive years).
Momentum = ultimate stationary population / current population.

References:
    Malthus, T.R. (1798). An Essay on the Principle of Population.
    Galor, O. (2005). From Stagnation to Growth: Unified Growth Theory.
        In Handbook of Economic Growth, Vol. 1A, pp. 171-293.
    Galor, O. (2011). Unified Growth Theory. Princeton University Press.
    Keyfitz, N. (1971). On the Momentum of Population Growth. Demography, 8(1).
    Solow, R.M. (1956). A Contribution to the Theory of Economic Growth.
        Quarterly Journal of Economics, 70(1), 65-94.

Score: Malthusian trap detected -> CRISIS, rapid unsustainable growth -> STRESS,
post-transition stable -> STABLE.
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class PopulationGrowth(LayerBase):
    layer_id = "l17"
    name = "Population Growth"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        country_iso3 = kwargs.get("country_iso3")

        # Population growth rate
        pop_growth_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SP.POP.GROW'
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

        # Total population
        pop_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SP.POP.TOTL'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # TFR for momentum calculation
        tfr_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SP.DYN.TFRT.IN'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Gross capital formation (% of GDP) for Solow
        gfcf_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NE.GDI.TOTL.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        if not pop_growth_rows and not gdppc_rows:
            return {"score": 50, "results": {"error": "no population/GDP data"}}

        def _index(rows):
            out: dict[str, dict[str, float]] = {}
            for r in rows:
                out.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]
            return out

        pop_g_data = _index(pop_growth_rows) if pop_growth_rows else {}
        gdppc_data = _index(gdppc_rows) if gdppc_rows else {}
        pop_data = _index(pop_rows) if pop_rows else {}
        tfr_data = _index(tfr_rows) if tfr_rows else {}
        gfcf_data = _index(gfcf_rows) if gfcf_rows else {}

        # --- Malthusian trap test ---
        # Within-country: test if GDP per capita growth is negatively correlated
        # with population growth (Malthusian), or positively (modern growth)
        malthusian = None
        if country_iso3:
            pg_c = pop_g_data.get(country_iso3, {})
            gdp_c = gdppc_data.get(country_iso3, {})
            if pg_c and gdp_c:
                common = sorted(set(pg_c.keys()) & set(gdp_c.keys()))
                if len(common) >= 10:
                    pg_vals = np.array([pg_c[y] for y in common if pg_c[y] is not None])
                    gdp_vals = np.array([
                        gdp_c[y] for y in common if gdp_c[y] is not None
                    ])
                    min_len = min(len(pg_vals), len(gdp_vals))
                    if min_len >= 10:
                        pg_vals = pg_vals[:min_len]
                        gdp_vals = gdp_vals[:min_len]

                        # GDP per capita growth rate
                        gdp_growth = np.diff(np.log(np.maximum(gdp_vals, 1))) * 100

                        # Correlation: pop growth vs GDP pc growth
                        pg_for_corr = pg_vals[1:]  # align with growth
                        if len(gdp_growth) >= 10:
                            corr, p_val = stats.pearsonr(pg_for_corr, gdp_growth)

                            # Malthusian: negative correlation (more pop -> lower income)
                            # Modern: positive or zero
                            malthusian = {
                                "correlation": round(float(corr), 4),
                                "p_value": round(float(p_val), 4),
                                "n_obs": len(gdp_growth),
                                "malthusian_trap": corr < -0.3 and p_val < 0.10,
                                "regime": (
                                    "malthusian" if corr < -0.3 and p_val < 0.10
                                    else "post-malthusian" if corr < 0
                                    else "modern_growth"
                                ),
                            }

        # --- Unified growth theory: classify country's growth regime ---
        unified_growth = None
        if country_iso3:
            gdp_c = gdppc_data.get(country_iso3, {})
            pg_c = pop_g_data.get(country_iso3, {})
            tfr_c = tfr_data.get(country_iso3, {})

            if gdp_c and pg_c:
                gdp_yrs = sorted(gdp_c.keys())
                gdp_vals = [gdp_c[y] for y in gdp_yrs if gdp_c[y] is not None]
                pg_yrs = sorted(pg_c.keys())
                pg_vals = [pg_c[y] for y in pg_yrs if pg_c[y] is not None]

                if len(gdp_vals) >= 5 and len(pg_vals) >= 5:
                    avg_gdp_growth = (
                        (np.log(max(gdp_vals[-1], 1)) - np.log(max(gdp_vals[0], 1)))
                        / max(len(gdp_vals) - 1, 1)
                        * 100
                    )
                    latest_pop_g = pg_vals[-1]
                    latest_tfr = None
                    if tfr_c:
                        tfr_yrs = sorted(tfr_c.keys())
                        latest_tfr = tfr_c[tfr_yrs[-1]]

                    epoch = self._classify_epoch(
                        avg_gdp_growth, latest_pop_g, latest_tfr
                    )
                    unified_growth = {
                        "avg_gdp_pc_growth": round(float(avg_gdp_growth), 2),
                        "latest_pop_growth": round(float(latest_pop_g), 2),
                        "latest_tfr": round(float(latest_tfr), 2) if latest_tfr else None,
                        "epoch": epoch,
                    }

        # --- Solow capital dilution effect ---
        # Higher population growth requires higher savings to maintain k*
        solow_dilution = None
        s_list, n_list, gdp_g_list = [], [], []
        for iso in set(gfcf_data.keys()) & set(pop_g_data.keys()) & set(gdppc_data.keys()):
            gfcf_c = gfcf_data[iso]
            pg_c = pop_g_data[iso]
            gdp_c = gdppc_data[iso]
            common = sorted(set(gfcf_c.keys()) & set(pg_c.keys()) & set(gdp_c.keys()))
            if common:
                yr = common[-1]
                s_val = gfcf_c[yr]
                n_val = pg_c[yr]
                g_val = gdp_c[yr]
                if (
                    s_val is not None
                    and n_val is not None
                    and g_val is not None
                    and g_val > 0
                ):
                    s_list.append(s_val / 100.0)  # savings rate as fraction
                    n_list.append(n_val / 100.0)  # population growth as fraction
                    gdp_g_list.append(np.log(g_val))

        if len(s_list) >= 20:
            s_arr = np.array(s_list)
            n_arr = np.array(n_list)
            gdp_arr = np.array(gdp_g_list)

            # Solow: log(y*) = const + alpha/(1-alpha)*log(s) - alpha/(1-alpha)*log(n+g+delta)
            # Approximate: test sign of pop growth coefficient on income
            delta_g = 0.05  # depreciation + tech growth ~5%
            effective_n = n_arr + delta_g
            log_s = np.log(np.maximum(s_arr, 0.01))
            log_ng = np.log(np.maximum(effective_n, 0.001))

            X = np.column_stack([np.ones(len(gdp_arr)), log_s, log_ng])
            beta = np.linalg.lstsq(X, gdp_arr, rcond=None)[0]
            y_hat = X @ beta
            ss_res = np.sum((gdp_arr - y_hat) ** 2)
            ss_tot = np.sum((gdp_arr - np.mean(gdp_arr)) ** 2)
            r_sq = 1 - ss_res / ss_tot if ss_tot > 0 else 0

            # Implied alpha from coefficients
            # beta_s = alpha/(1-alpha), so alpha = beta_s / (1 + beta_s)
            implied_alpha = beta[1] / (1 + beta[1]) if beta[1] > 0 else None

            solow_dilution = {
                "savings_coef": round(float(beta[1]), 4),
                "pop_growth_coef": round(float(beta[2]), 4),
                "r_squared": round(float(r_sq), 4),
                "n_countries": len(s_list),
                "implied_alpha": round(float(implied_alpha), 3) if implied_alpha else None,
                "dilution_confirmed": beta[2] < 0,
            }

        # --- Population momentum ---
        momentum = None
        if country_iso3 and country_iso3 in tfr_data and country_iso3 in pop_data:
            tfr_c = tfr_data[country_iso3]
            pop_c = pop_data[country_iso3]
            tfr_yrs = sorted(tfr_c.keys())
            pop_yrs = sorted(pop_c.keys())
            if tfr_yrs and pop_yrs:
                latest_tfr = tfr_c[tfr_yrs[-1]]
                latest_pop = pop_c[pop_yrs[-1]]
                if latest_tfr is not None and latest_pop is not None:
                    # Keyfitz approximation: momentum ~ (e0 * NRR) / (mu * l(mu))
                    # Simplified: if TFR > replacement, momentum > 1
                    replacement = 2.1
                    nrr_approx = (latest_tfr / replacement) if latest_tfr > 0 else 1.0
                    # Rough momentum: born_alive * NRR^(mean_generation/period)
                    # Simplified Keyfitz: M ~ (birth_rate * e0) / (crude_rate_reproduction)
                    # Use simpler approximation: M ~ TFR / replacement when TFR > replacement
                    momentum_factor = max(1.0, nrr_approx)
                    momentum = {
                        "latest_tfr": round(float(latest_tfr), 2),
                        "nrr_approximation": round(float(nrr_approx), 3),
                        "momentum_factor": round(float(momentum_factor), 3),
                        "above_replacement": latest_tfr > replacement,
                        "implied_growth_inertia": momentum_factor > 1.0,
                    }

        # --- Score ---
        score = 50.0
        if unified_growth:
            epoch = unified_growth["epoch"]
            if epoch == "malthusian":
                score = 85.0
            elif epoch == "post_malthusian":
                score = 60.0
            elif epoch == "modern_sustained":
                score = 25.0
            elif epoch == "demographic_transition_complete":
                score = 15.0
            elif epoch == "population_decline":
                score = 55.0  # potential labor shortage

        if malthusian and malthusian["malthusian_trap"]:
            score = max(score, 80.0)

        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "results": {
                "malthusian_test": malthusian,
                "unified_growth": unified_growth,
                "solow_capital_dilution": solow_dilution,
                "population_momentum": momentum,
                "country_iso3": country_iso3,
            },
        }

    @staticmethod
    def _classify_epoch(
        avg_gdp_growth: float, pop_growth: float, tfr: float | None
    ) -> str:
        """Classify Galor unified growth theory epoch."""
        if avg_gdp_growth < 0.5 and pop_growth > 2.0:
            return "malthusian"
        if avg_gdp_growth > 0 and pop_growth > 1.5:
            return "post_malthusian"
        if avg_gdp_growth > 1.0 and pop_growth > 0:
            if tfr is not None and tfr < 2.5:
                return "demographic_transition_complete"
            return "modern_sustained"
        if pop_growth < 0:
            return "population_decline"
        return "transitional"
