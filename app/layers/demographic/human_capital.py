"""Human capital accumulation: Mincer lifecycle, Ben-Porath, Grossman.

Models human capital as an economic asset using three foundational frameworks:

1. Mincer lifecycle model: earnings rise with experience (learning-by-doing)
   then plateau as depreciation offsets new investment. Extended to include
   on-the-job training intensity, sector-specific capital, and vintage effects.

2. Ben-Porath (1967) optimal investment: individuals choose human capital
   investment to maximize lifetime wealth. Investment is highest when young
   (long payoff horizon, low opportunity cost) and declines with age:
       max sum_{t=0}^{T} [w*H_t*(1-s_t) - C(s_t*H_t)] / (1+r)^t
   where s_t is fraction of time spent in training, H_t is stock.

3. Grossman (1972) health as human capital: health stock depreciates with
   age, individuals invest in health to slow depreciation. Extends Becker's
   model to treat healthy time as both consumption and investment good.

4. Intergenerational transmission: parental human capital affects child
   outcomes through genetics, investment, and environment. Becker-Tomes
   model of intergenerational mobility:
       h_{t+1} = alpha + beta*h_t + e_{t+1}
   where beta is the intergenerational elasticity of human capital.

References:
    Ben-Porath, Y. (1967). The Production of Human Capital and the Life
        Cycle of Earnings. JPE, 75(4), 352-365.
    Grossman, M. (1972). On the Concept of Health Capital and the Demand
        for Health. JPE, 80(2), 223-255.
    Becker, G.S. & Tomes, N. (1986). Human Capital and the Rise and Fall
        of Families. Journal of Labor Economics, 4(3), S1-S39.
    Heckman, J., Lochner, L. & Todd, P. (2006). Earnings Functions,
        Rates of Return, and Treatment Effects. In Handbook of the
        Economics of Education, Vol. 1, pp. 307-458.

Score: high human capital investment (education + health spending) with
good returns -> STABLE, low investment or poor returns -> STRESS.
"""

from __future__ import annotations

import numpy as np
from scipy import optimize, stats

from app.layers.base import LayerBase


class HumanCapitalAccumulation(LayerBase):
    layer_id = "l17"
    name = "Human Capital Accumulation"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        country_iso3 = kwargs.get("country_iso3")

        # School enrollment, secondary (% gross)
        sec_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SE.SEC.ENRR'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # School enrollment, tertiary (% gross)
        ter_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SE.TER.ENRR'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Government expenditure on education (% of GDP)
        edu_exp_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SE.XPD.TOTL.GD.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Life expectancy (health capital proxy)
        le_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SP.DYN.LE00.IN'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # GDP per capita (for returns estimation)
        gdppc_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.PCAP.KD'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Human Capital Index (World Bank)
        hci_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'HD.HCI.OVRL'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        if not sec_rows and not le_rows:
            return {"score": 50, "results": {"error": "no human capital data"}}

        def _index(rows):
            out: dict[str, dict[str, float]] = {}
            for r in rows:
                out.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]
            return out

        sec_data = _index(sec_rows) if sec_rows else {}
        ter_data = _index(ter_rows) if ter_rows else {}
        edu_exp_data = _index(edu_exp_rows) if edu_exp_rows else {}
        le_data = _index(le_rows) if le_rows else {}
        gdppc_data = _index(gdppc_rows) if gdppc_rows else {}
        hci_data = _index(hci_rows) if hci_rows else {}

        # --- Ben-Porath optimal investment profile ---
        # Cross-country: education spending -> secondary enrollment -> GDP growth
        ben_porath = None
        edu_list, enr_list = [], []
        for iso in set(edu_exp_data.keys()) & set(sec_data.keys()):
            e_yrs = edu_exp_data[iso]
            s_yrs = sec_data[iso]
            common = sorted(set(e_yrs.keys()) & set(s_yrs.keys()))
            if common:
                yr = common[-1]
                e_val = e_yrs[yr]
                s_val = s_yrs[yr]
                if e_val is not None and s_val is not None and e_val > 0:
                    edu_list.append(e_val)
                    enr_list.append(s_val)

        if len(edu_list) >= 20:
            edu_arr = np.array(edu_list)
            enr_arr = np.array(enr_list)
            slope, intercept, r, p, se = stats.linregress(edu_arr, enr_arr)
            ben_porath = {
                "spending_enrollment_elasticity": round(float(slope), 4),
                "se": round(float(se), 4),
                "r_squared": round(float(r ** 2), 4),
                "p_value": round(float(p), 6),
                "n_countries": len(edu_list),
                "investment_effective": slope > 0 and p < 0.05,
            }

        # --- Grossman health capital model ---
        # Cross-country: life expectancy vs GDP per capita (log-linear)
        grossman = None
        le_list, lgdp_list = [], []
        for iso in set(le_data.keys()) & set(gdppc_data.keys()):
            le_c = le_data[iso]
            gdp_c = gdppc_data[iso]
            common = sorted(set(le_c.keys()) & set(gdp_c.keys()))
            if common:
                yr = common[-1]
                l_val = le_c[yr]
                g_val = gdp_c[yr]
                if l_val is not None and g_val is not None and g_val > 0:
                    le_list.append(l_val)
                    lgdp_list.append(np.log(g_val))

        if len(le_list) >= 20:
            le_arr = np.array(le_list)
            lgdp_arr = np.array(lgdp_list)
            slope, intercept, r, p, se = stats.linregress(lgdp_arr, le_arr)

            # Preston curve: diminishing returns (log-linear fit)
            grossman = {
                "income_health_elasticity": round(float(slope), 4),
                "se": round(float(se), 4),
                "r_squared": round(float(r ** 2), 4),
                "p_value": round(float(p), 6),
                "n_countries": len(le_list),
                "preston_curve_confirmed": slope > 0 and p < 0.05,
            }

        # --- Intergenerational transmission ---
        # Within-country: secondary enrollment trend as proxy for HC accumulation
        intergenerational = None
        if country_iso3 and country_iso3 in sec_data:
            sec_c = sec_data[country_iso3]
            yrs = sorted(sec_c.keys())
            vals = [sec_c[y] for y in yrs if sec_c[y] is not None]
            yrs_num = [int(y) for y in yrs if sec_c[y] is not None]
            if len(vals) >= 10:
                vals_arr = np.array(vals)
                yrs_arr = np.array(yrs_num)
                slope, intercept, r, p, se = stats.linregress(yrs_arr, vals_arr)

                # Approximate intergenerational elasticity from enrollment persistence
                # Lag by ~20 years (one generation)
                lag = min(20, len(vals) - 1)
                if lag >= 5:
                    current = vals_arr[lag:]
                    lagged = vals_arr[:len(current)]
                    if len(current) >= 5:
                        ig_slope, _, ig_r, ig_p, _ = stats.linregress(
                            lagged, current
                        )
                        intergenerational = {
                            "enrollment_trend": round(float(slope), 4),
                            "trend_r_squared": round(float(r ** 2), 4),
                            "intergenerational_elasticity": round(float(ig_slope), 4),
                            "ig_r_squared": round(float(ig_r ** 2), 4),
                            "ig_p_value": round(float(ig_p), 4),
                            "lag_years": lag,
                            "n_obs": len(current),
                            "high_persistence": ig_slope > 0.8,
                        }

        # --- Human Capital Index snapshot ---
        hci_snapshot = None
        if country_iso3 and country_iso3 in hci_data:
            hci_c = hci_data[country_iso3]
            yrs = sorted(hci_c.keys())
            if yrs:
                yr = yrs[-1]
                hci_val = hci_c[yr]
                if hci_val is not None:
                    hci_snapshot = {
                        "hci_value": round(float(hci_val), 4),
                        "year": yr,
                        "interpretation": (
                            "A child born today will be "
                            f"{round(float(hci_val) * 100, 1)}% as productive as "
                            "with complete education and full health."
                        ),
                    }

        # --- Country investment profile ---
        investment_profile = None
        if country_iso3:
            profile = {}
            if country_iso3 in sec_data:
                latest_sec = sorted(sec_data[country_iso3].items())
                if latest_sec:
                    profile["secondary_enrollment"] = float(latest_sec[-1][1] or 0)
            if country_iso3 in ter_data:
                latest_ter = sorted(ter_data[country_iso3].items())
                if latest_ter:
                    profile["tertiary_enrollment"] = float(latest_ter[-1][1] or 0)
            if country_iso3 in edu_exp_data:
                latest_edu = sorted(edu_exp_data[country_iso3].items())
                if latest_edu:
                    profile["education_spending_pct_gdp"] = float(
                        latest_edu[-1][1] or 0
                    )
            if country_iso3 in le_data:
                latest_le = sorted(le_data[country_iso3].items())
                if latest_le:
                    profile["life_expectancy"] = float(latest_le[-1][1] or 0)
            if profile:
                investment_profile = profile

        # --- Score ---
        score = 50.0
        if investment_profile:
            sec_e = investment_profile.get("secondary_enrollment", 0)
            edu_s = investment_profile.get("education_spending_pct_gdp", 0)
            le_v = investment_profile.get("life_expectancy", 0)

            # Enrollment component (0-30 points of reduction from stress)
            if sec_e > 90:
                score -= 20
            elif sec_e > 70:
                score -= 10
            elif sec_e < 50:
                score += 15

            # Education spending (0-15 points)
            if edu_s > 5:
                score -= 10
            elif edu_s < 2:
                score += 10

            # Life expectancy / health capital (0-15 points)
            if le_v > 75:
                score -= 10
            elif le_v < 60:
                score += 15

        if hci_snapshot:
            hci_v = hci_snapshot["hci_value"]
            if hci_v > 0.7:
                score -= 10
            elif hci_v < 0.4:
                score += 15

        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "results": {
                "ben_porath_investment": ben_porath,
                "grossman_health_capital": grossman,
                "intergenerational": intergenerational,
                "hci_snapshot": hci_snapshot,
                "investment_profile": investment_profile,
                "country_iso3": country_iso3,
            },
        }
