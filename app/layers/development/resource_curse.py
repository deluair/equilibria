"""Resource curse: natural resource dependence and economic growth.

Tests whether natural resource abundance (rents/GDP) is associated with
lower growth, weaker institutions, and Dutch disease symptoms. Examines
the Mehlum et al. (2006) hypothesis that the curse operates through
institutional quality.

Key references:
    Sachs, J. & Warner, A. (1995). Natural resource abundance and economic
        growth. NBER Working Paper 5398.
    Mehlum, H., Moene, K. & Torvik, R. (2006). Institutions and the resource
        curse. Economic Journal, 116(508), 1-20.
    van der Ploeg, F. (2011). Natural resources: curse or blessing? Journal of
        Economic Literature, 49(2), 366-420.
"""

from __future__ import annotations

import numpy as np
import statsmodels.api as sm

from app.layers.base import LayerBase


class ResourceCurse(LayerBase):
    layer_id = "l4"
    name = "Resource Curse"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Test resource curse hypothesis with institutional interaction.

        Fetches natural resource rents/GDP, GDP growth, and institutional
        quality. Tests direct resource-growth relationship and interaction
        with governance (Mehlum et al.).

        Returns dict with score, resource curse coefficient, institutional
        interaction, Dutch disease indicators, and country classification.
        """
        country_iso3 = kwargs.get("country_iso3")

        # Fetch total natural resource rents (% of GDP)
        rents_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.TOTL.RT.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # GDP growth
        growth_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.MKTP.KD.ZG'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Institutional quality (rule of law)
        inst_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'RL.EST'
              AND dp.date = (
                  SELECT MAX(dp2.date) FROM data_points dp2
                  WHERE dp2.series_id = ds.id
              )
            """
        )

        if not rents_rows or not growth_rows:
            return {"score": 50, "results": {"error": "no resource rents or growth data"}}

        # Build panel
        rents_data: dict[str, dict[str, float]] = {}
        for r in rents_rows:
            rents_data.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]

        growth_data: dict[str, dict[str, float]] = {}
        for r in growth_rows:
            growth_data.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]

        inst_dict = {r["country_iso3"]: r["value"] for r in inst_rows} if inst_rows else {}

        # Build estimation sample using country averages
        y_avg, rents_avg, inst_vals = [], [], []
        sample_isos = []

        for iso in set(rents_data.keys()) & set(growth_data.keys()):
            common = sorted(set(rents_data[iso].keys()) & set(growth_data[iso].keys()))
            if len(common) < 5:
                continue
            avg_g = np.mean([growth_data[iso][yr] for yr in common])
            avg_r = np.mean([rents_data[iso][yr] for yr in common])
            y_avg.append(avg_g)
            rents_avg.append(avg_r)
            sample_isos.append(iso)
            inst_vals.append(inst_dict.get(iso))

        if len(y_avg) < 20:
            return {"score": 50, "results": {"error": "insufficient countries"}}

        y = np.array(y_avg)
        rents = np.array(rents_avg)

        # Model 1: Baseline resource-growth relationship
        X1 = sm.add_constant(rents)
        m1 = sm.OLS(y, X1).fit(cov_type="HC1")

        # Model 2: With institutional interaction (Mehlum et al.)
        has_inst = [i for i, v in enumerate(inst_vals) if v is not None]
        interaction_results = None
        if len(has_inst) >= 20:
            y_inst = y[has_inst]
            rents_inst = rents[has_inst]
            inst = np.array([inst_vals[i] for i in has_inst])
            interaction = rents_inst * inst

            X2 = sm.add_constant(np.column_stack([rents_inst, inst, interaction]))
            m2 = sm.OLS(y_inst, X2).fit(cov_type="HC1")

            interaction_results = {
                "rents_coef": float(m2.params[1]),
                "inst_coef": float(m2.params[2]),
                "interaction_coef": float(m2.params[3]),
                "interaction_se": float(m2.bse[3]),
                "interaction_pval": float(m2.pvalues[3]),
                "r_sq": float(m2.rsquared),
                "n_obs": int(m2.nobs),
                "mehlum_hypothesis": float(m2.params[3]) > 0 and float(m2.pvalues[3]) < 0.10,
            }

        # Dutch disease check: manufacturing share decline with resource rents
        mfg_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NV.IND.MANF.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )
        dutch_disease = None
        if mfg_rows:
            mfg_data: dict[str, dict[str, float]] = {}
            for r in mfg_rows:
                mfg_data.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]

            dd_rents, dd_mfg = [], []
            for iso in set(rents_data.keys()) & set(mfg_data.keys()):
                common = sorted(set(rents_data[iso].keys()) & set(mfg_data[iso].keys()))
                if len(common) >= 3:
                    dd_rents.append(np.mean([rents_data[iso][yr] for yr in common]))
                    dd_mfg.append(np.mean([mfg_data[iso][yr] for yr in common]))

            if len(dd_rents) >= 15:
                X_dd = sm.add_constant(np.array(dd_rents))
                m_dd = sm.OLS(np.array(dd_mfg), X_dd).fit(cov_type="HC1")
                dutch_disease = {
                    "rents_on_manufacturing": float(m_dd.params[1]),
                    "pval": float(m_dd.pvalues[1]),
                    "r_sq": float(m_dd.rsquared),
                    "evidence": float(m_dd.params[1]) < 0 and float(m_dd.pvalues[1]) < 0.10,
                }

        # Classification: resource-dependent countries
        resource_dependent = [(iso, rents_avg[i]) for i, iso in enumerate(sample_isos) if rents_avg[i] > 10]
        resource_dependent.sort(key=lambda x: x[1], reverse=True)

        # Target country
        target_analysis = None
        if country_iso3 and country_iso3 in rents_data:
            latest_years = sorted(rents_data[country_iso3].keys())
            if latest_years:
                latest_rents = rents_data[country_iso3][latest_years[-1]]
                target_analysis = {
                    "resource_rents_pct_gdp": latest_rents,
                    "resource_dependent": latest_rents > 10,
                    "highly_dependent": latest_rents > 20,
                    "institutional_quality": inst_dict.get(country_iso3),
                }

        # Score: high resource dependence with weak institutions = stress
        if target_analysis:
            rents_val = target_analysis["resource_rents_pct_gdp"]
            inst_val = target_analysis["institutional_quality"]
            if rents_val > 20 and (inst_val is None or inst_val < 0):
                score = 80  # Resource curse risk
            elif rents_val > 10 and (inst_val is None or inst_val < 0):
                score = 65
            elif rents_val > 10:
                score = 45  # Dependent but governed
            elif rents_val > 5:
                score = 35
            else:
                score = 20  # Not resource dependent
        else:
            curse_coef = float(m1.params[1])
            if curse_coef < 0 and float(m1.pvalues[1]) < 0.05:
                score = 65  # Evidence of curse
            else:
                score = 40

        score = float(np.clip(score, 0, 100))

        results = {
            "baseline": {
                "rents_coef": float(m1.params[1]),
                "se": float(m1.bse[1]),
                "pval": float(m1.pvalues[1]),
                "r_sq": float(m1.rsquared),
                "n_obs": int(m1.nobs),
                "curse_evidence": float(m1.params[1]) < 0 and float(m1.pvalues[1]) < 0.10,
            },
            "institutional_interaction": interaction_results,
            "dutch_disease": dutch_disease,
            "resource_dependent_countries": resource_dependent[:10],
            "target": target_analysis,
            "country_iso3": country_iso3,
        }

        return {"score": score, "results": results}
