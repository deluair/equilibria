"""Aging economics: dependency, pensions, healthcare cost curves.

Models the economic consequences of population aging through old-age dependency
ratio projections, pension sustainability analysis via the Aaron (1966)
condition, healthcare expenditure age curves, and silver economy opportunities.

Aaron condition for PAYGO pension sustainability:
    r + g > n  =>  funded system dominates
    r + g < n  =>  PAYGO system dominates
where r = real interest rate, g = productivity growth, n = population growth.
When the support ratio (workers/retirees) falls below the implicit tax rate
threshold, the system becomes unsustainable.

Healthcare cost curve: per-capita health spending follows a J-curve with age,
accelerating sharply after 65. The "red herring" hypothesis (Zweifel et al.
1999) argues that proximity to death, not age per se, drives the cost curve.

Silver economy: aging creates new markets (assistive tech, leisure, healthcare),
labor reallocation, and potential for "longevity dividend" if healthy lifespan
extends proportionally.

References:
    Aaron, H. (1966). The Social Insurance Paradox. Canadian J. of Econ., 32(3).
    Lee, R. & Mason, A. (2011). Population Aging and the Generational Economy.
        Edward Elgar.
    Zweifel, P., Felder, S. & Meiers, M. (1999). Ageing of Population and
        Health Care Expenditure: A Red Herring? Health Economics, 8(6), 485-496.
    Bloom, D., Canning, D. & Fink, G. (2010). Implications of Population
        Ageing for Economic Growth. Oxford Review of Econ. Policy, 26(4).

Score: very high dependency (>40%) -> CRISIS, moderate (20-30%) -> WATCH,
low (<15%) -> STABLE. Aaron condition failure adds stress.
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class AgingEconomics(LayerBase):
    layer_id = "l17"
    name = "Aging Economics"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        country_iso3 = kwargs.get("country_iso3")

        # Old-age dependency ratio (% of working-age 15-64)
        dep_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SP.POP.DPND.OL'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Population ages 65+ (% of total)
        pop65_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SP.POP.65UP.TO.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Life expectancy at birth
        le_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SP.DYN.LE00.IN'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # GDP per capita growth (for Aaron condition)
        gdp_growth_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.PCAP.KD.ZG'
            ORDER BY ds.country_iso3, dp.date
            """
        )

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

        if not dep_rows and not pop65_rows:
            return {"score": 50, "results": {"error": "no aging demographic data"}}

        def _index(rows):
            out: dict[str, dict[str, float]] = {}
            for r in rows:
                out.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]
            return out

        dep_data = _index(dep_rows) if dep_rows else {}
        pop65_data = _index(pop65_rows) if pop65_rows else {}
        le_data = _index(le_rows) if le_rows else {}
        gdp_g_data = _index(gdp_growth_rows) if gdp_growth_rows else {}
        pop_g_data = _index(pop_growth_rows) if pop_growth_rows else {}

        # --- Dependency ratio trend and projection ---
        dependency = None
        latest_dep = None
        if country_iso3 and country_iso3 in dep_data:
            years_vals = sorted(dep_data[country_iso3].items())
            yrs = np.array([int(y) for y, v in years_vals if v is not None])
            vals = np.array([v for _, v in years_vals if v is not None])
            if len(vals) >= 5:
                slope, intercept, r, p, se = stats.linregress(yrs, vals)
                latest_dep = float(vals[-1])

                # Simple linear projection 10 and 20 years out
                last_yr = int(yrs[-1])
                proj_10 = float(slope * (last_yr + 10) + intercept)
                proj_20 = float(slope * (last_yr + 20) + intercept)

                dependency = {
                    "latest_ratio": latest_dep,
                    "year": last_yr,
                    "annual_change": round(float(slope), 4),
                    "r_squared": round(float(r ** 2), 4),
                    "projected_10yr": round(max(0, proj_10), 2),
                    "projected_20yr": round(max(0, proj_20), 2),
                    "aging_accelerating": slope > 0,
                    "n_years": len(vals),
                }

        # --- Aaron condition for PAYGO pension sustainability ---
        aaron = None
        if country_iso3:
            gdp_g_c = gdp_g_data.get(country_iso3, {})
            pop_g_c = pop_g_data.get(country_iso3, {})
            if gdp_g_c and pop_g_c:
                common_yrs = sorted(set(gdp_g_c.keys()) & set(pop_g_c.keys()))
                if len(common_yrs) >= 5:
                    # Use last 10 years average
                    recent = common_yrs[-10:]
                    avg_gdp_g = np.mean([gdp_g_c[y] for y in recent if gdp_g_c[y]])
                    avg_pop_g = np.mean([pop_g_c[y] for y in recent if pop_g_c[y]])

                    # Implicit real interest rate proxy: r ~ gdp_growth + 2% (rough)
                    # Aaron: PAYGO sustainable when n > r + g (simplified)
                    # More precisely: PAYGO dominates when pop_growth + wage_growth > r
                    # We approximate wage_growth ~ gdp_per_capita_growth
                    implicit_r = float(avg_gdp_g + 2.0)  # crude proxy
                    paygo_return = float(avg_pop_g + avg_gdp_g)

                    # Support ratio: inverse of dependency
                    support_ratio = None
                    if latest_dep and latest_dep > 0:
                        support_ratio = round(100.0 / latest_dep, 2)

                    aaron = {
                        "avg_gdp_growth": round(float(avg_gdp_g), 2),
                        "avg_pop_growth": round(float(avg_pop_g), 2),
                        "implicit_real_rate": round(implicit_r, 2),
                        "paygo_implicit_return": round(paygo_return, 2),
                        "aaron_favors_paygo": paygo_return > implicit_r,
                        "support_ratio": support_ratio,
                        "period": f"{recent[0]}-{recent[-1]}",
                    }

        # --- Healthcare cost curve (cross-country age-spending relationship) ---
        healthcare_aging = None
        pop65_list, he_list = [], []
        # Health expenditure as % of GDP (SH.XPD.CHEX.GD.ZS)
        he_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.XPD.CHEX.GD.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )
        he_data = _index(he_rows) if he_rows else {}

        for iso in set(pop65_data.keys()) & set(he_data.keys()):
            p65_yrs = pop65_data[iso]
            he_yrs = he_data[iso]
            common = sorted(set(p65_yrs.keys()) & set(he_yrs.keys()))
            if common:
                yr = common[-1]
                p_val = p65_yrs[yr]
                h_val = he_yrs[yr]
                if p_val is not None and h_val is not None and p_val > 0:
                    pop65_list.append(p_val)
                    he_list.append(h_val)

        if len(pop65_list) >= 20:
            pop65_arr = np.array(pop65_list)
            he_arr = np.array(he_list)
            slope, intercept, r, p, se = stats.linregress(pop65_arr, he_arr)
            healthcare_aging = {
                "age65_health_elasticity": round(float(slope), 4),
                "se": round(float(se), 4),
                "r_squared": round(float(r ** 2), 4),
                "p_value": round(float(p), 6),
                "n_countries": len(pop65_list),
                "cost_pressure": slope > 0,
            }

        # --- Silver economy indicator ---
        silver_economy = None
        if country_iso3 and country_iso3 in pop65_data and country_iso3 in le_data:
            p65_c = pop65_data[country_iso3]
            le_c = le_data[country_iso3]
            common = sorted(set(p65_c.keys()) & set(le_c.keys()))
            if common:
                yr = common[-1]
                p65_val = p65_c[yr]
                le_val = le_c[yr]
                if p65_val is not None and le_val is not None:
                    # Healthy aging index: life expectancy relative to 65+ share
                    # Higher LE with moderate 65+ share = better silver economy potential
                    healthy_aging_ratio = le_val / max(p65_val, 1.0)
                    silver_economy = {
                        "pop_65plus_pct": round(float(p65_val), 2),
                        "life_expectancy": round(float(le_val), 1),
                        "healthy_aging_ratio": round(float(healthy_aging_ratio), 2),
                        "year": yr,
                        "silver_economy_stage": (
                            "emerging" if p65_val < 7
                            else "maturing" if p65_val < 14
                            else "advanced" if p65_val < 21
                            else "super-aged"
                        ),
                    }

        # --- Score ---
        if latest_dep is not None:
            score = self._dependency_to_score(latest_dep)
            # Aaron condition failure adds stress
            if aaron and not aaron["aaron_favors_paygo"]:
                score = min(100.0, score + 10.0)
        else:
            score = 50.0

        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "results": {
                "dependency_ratio": dependency,
                "aaron_condition": aaron,
                "healthcare_cost_curve": healthcare_aging,
                "silver_economy": silver_economy,
                "country_iso3": country_iso3,
            },
        }

    @staticmethod
    def _dependency_to_score(dep_ratio: float) -> float:
        """Map old-age dependency ratio to score (0-100).

        Low dependency (<15%) is stable. High dependency (>40%) is crisis.
        """
        if dep_ratio < 10:
            return 10.0
        if dep_ratio < 15:
            return 10.0 + (dep_ratio - 10) * 2.0  # 10-20
        if dep_ratio < 25:
            return 20.0 + (dep_ratio - 15) * 2.0  # 20-40
        if dep_ratio < 35:
            return 40.0 + (dep_ratio - 25) * 2.5  # 40-65
        if dep_ratio < 45:
            return 65.0 + (dep_ratio - 35) * 2.5  # 65-90
        return min(100.0, 90.0 + (dep_ratio - 45) * 1.0)
