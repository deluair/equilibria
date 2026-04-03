"""Secular Stagnation analysis module.

Methodology
-----------
The secular stagnation hypothesis (Summers 2014, reviving Hansen 1939)
posits that advanced economies face a chronic shortfall of aggregate
demand due to a persistent decline in the natural rate of interest (r*),
making monetary policy unable to achieve full employment without financial
excess.

Analysis dimensions:

1. Natural rate decline (Summers hypothesis test):
   Estimate r* using the Laubach-Williams (2003) simplified approach:
   - r*_t = c + beta * g_t + e_t  (potential growth is the main driver)
   - Compare r* estimates across sub-periods for decline test.

2. Demographic drag: working-age population growth and old-age dependency
   ratio trend. Aging populations raise desired savings and depress
   investment demand (Eggertsson & Mehrotra 2014).

3. Savings glut evidence: private savings rate trend, corporate cash
   holdings, global current account surplus accumulation.

4. Investment drought: trend in investment-to-GDP ratio and real
   long-run neutral rate from yield curve (10yr - expected inflation).

Score (0-100): higher score indicates stronger secular stagnation
evidence -- declining r*, aging demographics, rising savings, falling
investment -- requiring more unconventional policy.

References:
    Summers, L.H. (2014). "U.S. Economic Prospects: Secular Stagnation,
        Hysteresis, and the Zero Lower Bound." Business Economics, 49(2).
    Laubach, T. and Williams, J.C. (2003). "Measuring the Natural Rate
        of Interest." Review of Economics and Statistics, 85(4), 1063-1070.
    Eggertsson, G.B. and Mehrotra, N.R. (2014). "A Model of Secular
        Stagnation." NBER Working Paper 20574.
    Rachel, L. and Summers, L.H. (2019). "On Secular Stagnation in the
        Industrialized World." Brookings Papers on Economic Activity.
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class SecularStagnation(LayerBase):
    layer_id = "l2"
    name = "Secular Stagnation"

    async def compute(self, db, **kwargs) -> dict:
        """Test secular stagnation hypothesis.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country    : str  - ISO3 country code
            split_year : int  - year to split pre/post samples (default 2000)
        """
        country = kwargs.get("country", "USA")
        split_year = int(kwargs.get("split_year", 2000))

        series_map = {
            "real_rate_lt":   f"REAL_RATE_LT_{country}",      # long-run real rate
            "gdp_pot_growth": f"POT_GDP_GROWTH_{country}",    # potential GDP growth
            "savings_rate":   f"PRIV_SAVINGS_RATE_{country}",
            "invest_gdp":     f"INVEST_GDP_{country}",
            "workingage_pop": f"WORKING_AGE_POP_GROWTH_{country}",
            "depend_ratio":   f"OLD_AGE_DEP_RATIO_{country}",
            "nominal_rate_lt": f"NOMINAL_RATE_LT_{country}",
            "inflation":       f"INFLATION_{country}",
        }

        data: dict[str, np.ndarray] = {}
        dates_map: dict[str, list] = {}
        for label, code in series_map.items():
            rows = await db.execute_fetchall(
                "SELECT date, value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE code = ?) ORDER BY date",
                (code,),
            )
            if rows:
                dates_map[label] = [r[0] for r in rows]
                data[label] = np.array([float(r[1]) for r in rows])

        if len(data) < 2:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"Insufficient data for secular stagnation analysis of {country}",
            }

        results: dict = {"country": country}

        # --- 1. Natural rate decline (Laubach-Williams simplified) ---
        nat_rate = {}
        nat_rate_declined = False
        r_star_latest = None
        if "real_rate_lt" in data and len(data["real_rate_lt"]) >= 10:
            rr = data["real_rate_lt"]
            dates_rr = dates_map.get("real_rate_lt", [])
            n_rr = len(rr)

            # Extract year from date string (first 4 chars)
            years_rr = []
            for d in dates_rr:
                try:
                    years_rr.append(int(str(d)[:4]))
                except (ValueError, TypeError):
                    years_rr.append(0)

            years_arr = np.array(years_rr)

            # Split at split_year
            pre_mask = years_arr <= split_year if len(years_arr) == n_rr else np.ones(n_rr, bool)
            post_mask = ~pre_mask

            pre_mean = float(np.mean(rr[pre_mask])) if pre_mask.sum() > 2 else None
            post_mean = float(np.mean(rr[post_mask])) if post_mask.sum() > 2 else None

            # Overall trend
            x_t = np.arange(n_rr, dtype=float)
            slope_rr, _, r_rr, p_rr, _ = stats.linregress(x_t, rr)

            r_star_latest = float(rr[-1])
            nat_rate_declined = float(slope_rr) < -0.05

            nat_rate = {
                "r_star_latest": round(r_star_latest, 3),
                "pre_split_mean": round(pre_mean, 3) if pre_mean is not None else None,
                "post_split_mean": round(post_mean, 3) if post_mean is not None else None,
                "trend_slope_annual": round(float(slope_rr), 5),
                "r_squared": round(r_rr ** 2, 4),
                "p_value": round(float(p_rr), 4),
                "declining": nat_rate_declined,
                "split_year": split_year,
            }

            # Laubach-Williams: r* = c + beta * potential growth
            if "gdp_pot_growth" in data:
                pot_g = data["gdp_pot_growth"]
                n_lw = min(n_rr, len(pot_g))
                if n_lw >= 10:
                    X_lw = np.column_stack([np.ones(n_lw), pot_g[-n_lw:]])
                    beta_lw = np.linalg.lstsq(X_lw, rr[-n_lw:], rcond=None)[0]
                    resid_lw = rr[-n_lw:] - X_lw @ beta_lw
                    r2_lw = 1 - float(np.sum(resid_lw ** 2)) / max(
                        float(np.sum((rr[-n_lw:] - np.mean(rr[-n_lw:])) ** 2)), 1e-8
                    )
                    nat_rate["laubach_williams"] = {
                        "constant": round(float(beta_lw[0]), 4),
                        "growth_coefficient": round(float(beta_lw[1]), 4),
                        "r_squared": round(r2_lw, 4),
                        "interpretation": (
                            "beta > 1: r* more sensitive to growth than expected"
                            if beta_lw[1] > 1 else "beta < 1: moderate growth-r* link"
                        ),
                    }
        else:
            nat_rate = {"note": "long-run real rate data unavailable"}

        results["natural_rate"] = nat_rate

        # --- 2. Demographic drag ---
        demog = {}
        demog_drag_score = 0.0
        if "workingage_pop" in data and len(data["workingage_pop"]) >= 5:
            wap = data["workingage_pop"]
            latest_wap_growth = float(wap[-1])
            x_t = np.arange(len(wap), dtype=float)
            slope_wap, _, r_wap, p_wap, _ = stats.linregress(x_t, wap)
            demog["working_age_pop_growth_latest"] = round(latest_wap_growth, 4)
            demog["working_age_growth_trend_slope"] = round(float(slope_wap), 6)
            demog["declining_working_age"] = float(slope_wap) < 0
            if float(slope_wap) < -0.1:
                demog_drag_score += 20
            elif latest_wap_growth < 0.5:
                demog_drag_score += 10

        if "depend_ratio" in data and len(data["depend_ratio"]) >= 5:
            dr = data["depend_ratio"]
            slope_dr, _, r_dr, _, _ = stats.linregress(np.arange(len(dr), dtype=float), dr)
            demog["old_age_dependency_latest"] = round(float(dr[-1]), 2)
            demog["dependency_trend_slope"] = round(float(slope_dr), 4)
            demog["aging_rapidly"] = float(slope_dr) > 0.5
            if float(slope_dr) > 0.5:
                demog_drag_score += 15

        if not demog:
            demog = {"note": "demographic data unavailable"}

        results["demographic_drag"] = demog

        # --- 3. Savings glut ---
        savings_glut = {}
        savings_glut_score = 0.0
        if "savings_rate" in data and len(data["savings_rate"]) >= 5:
            sr = data["savings_rate"]
            x_t = np.arange(len(sr), dtype=float)
            slope_sr, _, r_sr, p_sr, _ = stats.linregress(x_t, sr)
            savings_glut = {
                "savings_rate_latest": round(float(sr[-1]), 2),
                "savings_trend_slope": round(float(slope_sr), 4),
                "r_squared": round(r_sr ** 2, 4),
                "rising_savings": float(slope_sr) > 0.1,
            }
            if float(slope_sr) > 0.1:
                savings_glut_score += 15
            if float(sr[-1]) > 25:
                savings_glut_score += 10
        else:
            savings_glut = {"note": "savings rate data unavailable"}

        results["savings_glut"] = savings_glut

        # --- 4. Investment drought ---
        invest_drought = {}
        invest_drought_score = 0.0
        if "invest_gdp" in data and len(data["invest_gdp"]) >= 5:
            inv = data["invest_gdp"]
            x_t = np.arange(len(inv), dtype=float)
            slope_inv, _, r_inv, p_inv, _ = stats.linregress(x_t, inv)
            invest_drought = {
                "invest_gdp_latest": round(float(inv[-1]), 2),
                "invest_trend_slope": round(float(slope_inv), 4),
                "r_squared": round(r_inv ** 2, 4),
                "declining_investment": float(slope_inv) < -0.1,
            }
            if float(slope_inv) < -0.1:
                invest_drought_score += 15
        else:
            invest_drought = {"note": "investment/GDP data unavailable"}

        # Yield-based r* proxy
        if "nominal_rate_lt" in data and "inflation" in data:
            nom = data["nominal_rate_lt"]
            pi = data["inflation"]
            n_y = min(len(nom), len(pi))
            if n_y >= 5:
                real_ex_ante = nom[-n_y:] - pi[-n_y:]
                invest_drought["yield_implied_real_rate"] = round(float(real_ex_ante[-1]), 3)
                invest_drought["near_zero_bound"] = float(real_ex_ante[-1]) < 0.5

        results["investment_drought"] = invest_drought

        # --- Score ---
        # Natural rate declining
        nat_rate_penalty = 25 if nat_rate_declined else 0
        if r_star_latest is not None and r_star_latest < 0:
            nat_rate_penalty += 15
        elif r_star_latest is not None and r_star_latest < 1:
            nat_rate_penalty += 8

        score = float(np.clip(
            nat_rate_penalty + demog_drag_score + savings_glut_score + invest_drought_score,
            0, 100
        ))

        return {"score": round(score, 2), "results": results}
