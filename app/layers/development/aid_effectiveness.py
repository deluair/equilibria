"""Aid effectiveness: Burnside-Dollar (2000) framework.

Tests whether foreign aid promotes economic growth conditional on good
policy environments. Estimates the aid-policy interaction effect and
examines heterogeneity by governance quality.

Key references:
    Burnside, C. & Dollar, D. (2000). Aid, policies, and growth. AER, 90(4),
        847-868.
    Easterly, W., Levine, R. & Roodman, D. (2004). Aid, policies, and growth:
        comment. AER, 94(3), 774-780.
    Rajan, R. & Subramanian, A. (2008). Aid and growth: what does the
        cross-country evidence really show? REStat, 90(4), 643-665.
"""

from __future__ import annotations

import numpy as np
import statsmodels.api as sm

from app.layers.base import LayerBase


class AidEffectiveness(LayerBase):
    layer_id = "l4"
    name = "Aid Effectiveness"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Estimate aid-growth relationship with policy interaction.

        Fetches ODA/GDP ratio, GDP growth, and policy indicators. Tests
        the Burnside-Dollar hypothesis that aid is effective in good
        policy environments.

        Returns dict with score, baseline aid-growth coefficient,
        interaction effect, and heterogeneous effects by governance.
        """
        country_iso3 = kwargs.get("country_iso3")

        # Fetch ODA as % of GNI
        aid_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'DT.ODA.ODAT.GN.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Fetch GDP growth
        growth_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.MKTP.KD.ZG'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        if not aid_rows or not growth_rows:
            return {"score": 50, "results": {"error": "no aid or growth data"}}

        # Build country-year panel
        aid_data: dict[str, dict[str, float]] = {}
        for r in aid_rows:
            aid_data.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]

        growth_data: dict[str, dict[str, float]] = {}
        for r in growth_rows:
            growth_data.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]

        # Fetch policy proxy: inflation (lower = better policy)
        inflation_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'FP.CPI.TOTL.ZG'
            ORDER BY ds.country_iso3, dp.date
            """
        )
        inflation_data: dict[str, dict[str, float]] = {}
        for r in inflation_rows:
            inflation_data.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]

        # Fetch trade openness as policy indicator
        openness_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NE.TRD.GNFS.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )
        openness_data: dict[str, dict[str, float]] = {}
        for r in openness_rows:
            openness_data.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]

        # Build estimation sample
        y_list, aid_list, policy_list, interact_list = [], [], [], []
        obs_countries = []

        for iso in set(aid_data.keys()) & set(growth_data.keys()):
            common_years = sorted(set(aid_data[iso].keys()) & set(growth_data[iso].keys()))
            for yr in common_years:
                aid_val = aid_data[iso][yr]
                growth_val = growth_data[iso][yr]
                if aid_val is None or growth_val is None:
                    continue

                # Construct policy index: standardized combination
                policy_components = []
                if iso in inflation_data and yr in inflation_data[iso]:
                    # Invert inflation: lower = better
                    policy_components.append(-inflation_data[iso][yr])
                if iso in openness_data and yr in openness_data[iso]:
                    policy_components.append(openness_data[iso][yr])

                if not policy_components:
                    continue

                policy = float(np.mean(policy_components))

                y_list.append(growth_val)
                aid_list.append(aid_val)
                policy_list.append(policy)
                interact_list.append(aid_val * policy)
                obs_countries.append(iso)

        if len(y_list) < 30:
            return {"score": 50, "results": {"error": "insufficient observations for aid-growth estimation"}}

        y = np.array(y_list)
        aid = np.array(aid_list)
        policy = np.array(policy_list)
        interaction = np.array(interact_list)

        # Standardize policy for interpretability
        policy_mean = np.mean(policy)
        policy_std = np.std(policy)
        if policy_std > 0:
            policy_z = (policy - policy_mean) / policy_std
            interaction_z = aid * policy_z
        else:
            policy_z = policy
            interaction_z = interaction

        # Model 1: Baseline aid-growth
        X1 = sm.add_constant(aid)
        m1 = sm.OLS(y, X1).fit(cov_type="HC1")

        # Model 2: Aid + policy
        X2 = sm.add_constant(np.column_stack([aid, policy_z]))
        m2 = sm.OLS(y, X2).fit(cov_type="HC1")

        # Model 3: Burnside-Dollar with interaction
        X3 = sm.add_constant(np.column_stack([aid, policy_z, interaction_z]))
        m3 = sm.OLS(y, X3).fit(cov_type="HC1")

        # Heterogeneous effects: split by governance quality
        policy_median = np.median(policy_z)
        high_policy = policy_z >= policy_median
        low_policy = ~high_policy

        het_results = {}
        for label, mask in [("good_policy", high_policy), ("bad_policy", low_policy)]:
            if np.sum(mask) >= 15:
                X_sub = sm.add_constant(aid[mask])
                m_sub = sm.OLS(y[mask], X_sub).fit(cov_type="HC1")
                het_results[label] = {
                    "aid_coef": float(m_sub.params[1]),
                    "se": float(m_sub.bse[1]),
                    "pval": float(m_sub.pvalues[1]),
                    "n_obs": int(m_sub.nobs),
                    "r_sq": float(m_sub.rsquared),
                }

        # Target country analysis
        target_analysis = None
        if country_iso3 and country_iso3 in aid_data:
            latest_years = sorted(aid_data[country_iso3].keys())[-5:]
            if latest_years:
                avg_aid = np.mean([aid_data[country_iso3][y] for y in latest_years
                                   if y in aid_data[country_iso3]])
                avg_growth_val = np.mean([growth_data[country_iso3][y] for y in latest_years
                                          if country_iso3 in growth_data and y in growth_data[country_iso3]])
                target_analysis = {
                    "avg_aid_pct_gni": float(avg_aid),
                    "avg_growth": float(avg_growth_val),
                    "years": latest_years,
                    "aid_dependent": avg_aid > 10,
                }

        # Score: high aid dependence with low growth = stress
        # Effective aid (positive interaction in good policy) = moderate
        if target_analysis:
            if target_analysis["aid_dependent"] and target_analysis["avg_growth"] < 2:
                score = 75
            elif target_analysis["aid_dependent"]:
                score = 55
            elif target_analysis["avg_aid_pct_gni"] > 5:
                score = 45
            else:
                score = 30
        else:
            # Use interaction coefficient significance
            interact_pval = float(m3.pvalues[3]) if len(m3.params) > 3 else 1.0
            interact_coef = float(m3.params[3]) if len(m3.params) > 3 else 0
            if interact_coef > 0 and interact_pval < 0.05:
                score = 40  # BD hypothesis holds: aid works with good policy
            else:
                score = 55

        score = float(np.clip(score, 0, 100))

        results = {
            "baseline": {
                "aid_coef": float(m1.params[1]),
                "se": float(m1.bse[1]),
                "pval": float(m1.pvalues[1]),
                "r_sq": float(m1.rsquared),
                "n_obs": int(m1.nobs),
            },
            "with_policy": {
                "aid_coef": float(m2.params[1]),
                "policy_coef": float(m2.params[2]),
                "r_sq": float(m2.rsquared),
            },
            "burnside_dollar": {
                "aid_coef": float(m3.params[1]),
                "policy_coef": float(m3.params[2]),
                "interaction_coef": float(m3.params[3]) if len(m3.params) > 3 else None,
                "interaction_se": float(m3.bse[3]) if len(m3.params) > 3 else None,
                "interaction_pval": float(m3.pvalues[3]) if len(m3.params) > 3 else None,
                "r_sq": float(m3.rsquared),
                "n_obs": int(m3.nobs),
            },
            "heterogeneous": het_results,
            "target": target_analysis,
            "country_iso3": country_iso3,
        }

        return {"score": score, "results": results}
