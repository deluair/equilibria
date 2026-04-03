"""Demographic dividend: age structure effects on economic growth.

Estimates the growth effects of changing age structure through dependency
ratio dynamics. Distinguishes between the first demographic dividend
(rising working-age share) and second dividend (longevity-induced
capital accumulation).

Key references:
    Bloom, D. & Williamson, J. (1998). Demographic transitions and economic
        miracles in emerging Asia. World Bank Economic Review, 12(3), 419-455.
    Mason, A. & Lee, R. (2006). Reform and support systems for the elderly in
        developing countries. Genus, 62(2), 11-35.
    Canning, D., Raja, S. & Yazbeck, A. (2015). Africa's Demographic
        Transition: Dividend or Disaster? World Bank.
"""

from __future__ import annotations

import numpy as np
import statsmodels.api as sm

from app.layers.base import LayerBase


class DemographicDividend(LayerBase):
    layer_id = "l4"
    name = "Demographic Dividend"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Estimate demographic dividend from age structure dynamics.

        Fetches working-age population share, dependency ratios, fertility
        rate, and life expectancy. Estimates the growth effect of
        demographic transition phases.

        Returns dict with score, first and second dividend estimates,
        dependency ratio trajectory, and demographic window assessment.
        """
        country_iso3 = kwargs.get("country_iso3")

        # Fetch age dependency ratio (% of working-age population)
        dep_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SP.POP.DPND'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # GDP growth
        growth_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.PCAP.KD.ZG'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Fertility rate
        fertility_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SP.DYN.TFRT.IN'
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

        if not dep_rows or not growth_rows:
            return {"score": 50, "results": {"error": "no dependency or growth data"}}

        # Build panel
        dep_data: dict[str, dict[str, float]] = {}
        for r in dep_rows:
            dep_data.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]

        growth_data: dict[str, dict[str, float]] = {}
        for r in growth_rows:
            growth_data.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]

        fertility_data: dict[str, dict[str, float]] = {}
        for r in fertility_rows:
            fertility_data.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]

        le_data: dict[str, dict[str, float]] = {}
        for r in le_rows:
            le_data.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]

        # First dividend estimation: dependency ratio change -> growth
        y_list, dep_change_list, dep_level_list = [], [], []
        obs_countries = []

        for iso in set(dep_data.keys()) & set(growth_data.keys()):
            dep_years = sorted(dep_data[iso].keys())
            growth_years = sorted(growth_data[iso].keys())
            common = sorted(set(dep_years) & set(growth_years))

            if len(common) < 5:
                continue

            for i in range(1, len(common)):
                yr = common[i]
                prev_yr = common[i - 1]
                dep_change = dep_data[iso][yr] - dep_data[iso][prev_yr]
                y_list.append(growth_data[iso][yr])
                dep_change_list.append(dep_change)
                dep_level_list.append(dep_data[iso][yr])
                obs_countries.append(iso)

        first_dividend = None
        if len(y_list) >= 30:
            y = np.array(y_list)
            dep_change = np.array(dep_change_list)
            dep_level = np.array(dep_level_list)

            # Model: growth = a + b1*delta_dependency + b2*dependency_level + e
            X = sm.add_constant(np.column_stack([dep_change, dep_level]))
            m = sm.OLS(y, X).fit(cov_type="HC1")

            first_dividend = {
                "dep_change_coef": float(m.params[1]),
                "dep_change_se": float(m.bse[1]),
                "dep_change_pval": float(m.pvalues[1]),
                "dep_level_coef": float(m.params[2]),
                "dep_level_pval": float(m.pvalues[2]),
                "r_sq": float(m.rsquared),
                "n_obs": int(m.nobs),
                "dividend_active": float(m.params[1]) < 0 and float(m.pvalues[1]) < 0.10,
            }

        # Target country demographic window analysis
        target_analysis = None
        if country_iso3 and country_iso3 in dep_data:
            dep_ts = sorted(dep_data[country_iso3].items(), key=lambda x: x[0])
            if len(dep_ts) >= 5:
                years = [y for y, _ in dep_ts]
                vals = [v for _, v in dep_ts]

                # Find demographic window: period where dependency is declining
                changes = np.diff(vals)
                declining = changes < 0

                # Current trajectory
                recent_trend = np.mean(changes[-5:]) if len(changes) >= 5 else np.mean(changes)

                # Window opening: first sustained decline
                window_open = None
                window_close = None
                for i in range(len(declining)):
                    if declining[i] and window_open is None:
                        if i + 2 < len(declining) and declining[i + 1]:
                            window_open = years[i]
                    if not declining[i] and window_open is not None and window_close is None:
                        if i + 2 < len(declining) and not declining[i + 1]:
                            window_close = years[i]

                # Fertility data for this country
                fertility_ts = None
                if country_iso3 in fertility_data:
                    f_sorted = sorted(fertility_data[country_iso3].items(), key=lambda x: x[0])
                    if f_sorted:
                        fertility_ts = {
                            "earliest": {"year": f_sorted[0][0], "rate": f_sorted[0][1]},
                            "latest": {"year": f_sorted[-1][0], "rate": f_sorted[-1][1]},
                            "below_replacement": f_sorted[-1][1] < 2.1,
                        }

                # Life expectancy trajectory
                le_ts = None
                if country_iso3 in le_data:
                    l_sorted = sorted(le_data[country_iso3].items(), key=lambda x: x[0])
                    if l_sorted:
                        le_ts = {
                            "earliest": {"year": l_sorted[0][0], "value": l_sorted[0][1]},
                            "latest": {"year": l_sorted[-1][0], "value": l_sorted[-1][1]},
                            "gain": l_sorted[-1][1] - l_sorted[0][1],
                        }

                # Phase classification
                latest_dep = vals[-1]
                if recent_trend < -0.5:
                    phase = "opening"  # Window opening
                elif recent_trend < 0:
                    phase = "open"  # Window open
                elif recent_trend < 0.5:
                    phase = "closing"  # Window closing
                else:
                    phase = "closed"  # Window closed or not yet open

                target_analysis = {
                    "current_dependency": latest_dep,
                    "recent_trend": float(recent_trend),
                    "phase": phase,
                    "window_open": window_open,
                    "window_close": window_close,
                    "fertility": fertility_ts,
                    "life_expectancy": le_ts,
                    "dependency_trajectory": {
                        "years": years[-10:],
                        "values": vals[-10:],
                    },
                }

        # Second dividend: longevity-induced savings
        second_dividend = None
        if le_rows:
            le_latest: dict[str, float] = {}
            for r in le_rows:
                iso = r["country_iso3"]
                if iso not in le_latest:
                    le_latest[iso] = r["value"]

            # Savings data
            savings_rows = await db.fetch_all(
                """
                SELECT ds.country_iso3, dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.series_id = 'NY.GDS.TOTL.ZS'
                  AND dp.date = (
                      SELECT MAX(dp2.date) FROM data_points dp2
                      WHERE dp2.series_id = ds.id
                  )
                """
            )
            if savings_rows:
                sav_dict = {r["country_iso3"]: r["value"] for r in savings_rows}
                common = [iso for iso in le_latest if iso in sav_dict]
                if len(common) >= 20:
                    le_arr = np.array([le_latest[c] for c in common])
                    sav_arr = np.array([sav_dict[c] for c in common])
                    X_s = sm.add_constant(le_arr)
                    m_s = sm.OLS(sav_arr, X_s).fit(cov_type="HC1")
                    second_dividend = {
                        "le_on_savings_coef": float(m_s.params[1]),
                        "pval": float(m_s.pvalues[1]),
                        "r_sq": float(m_s.rsquared),
                        "n_obs": int(m_s.nobs),
                    }

        # Score
        if target_analysis:
            phase = target_analysis["phase"]
            if phase == "open":
                score = 25  # Dividend window open
            elif phase == "opening":
                score = 30
            elif phase == "closing":
                score = 55
            else:
                score = 70  # Window closed or pre-transition
        else:
            score = 50

        score = float(np.clip(score, 0, 100))

        results = {
            "first_dividend": first_dividend,
            "second_dividend": second_dividend,
            "target": target_analysis,
            "country_iso3": country_iso3,
        }

        return {"score": score, "results": results}
