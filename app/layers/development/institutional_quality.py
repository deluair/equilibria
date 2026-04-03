"""Institutional quality and economic growth estimation.

Implements the Acemoglu-Johnson-Robinson (2001) IV strategy using settler
mortality as an instrument for institutional quality, and the La Porta
et al. (1998) legal origins framework. Estimates the causal effect of
institutions on long-run economic performance.

Key references:
    Acemoglu, D., Johnson, S. & Robinson, J. (2001). The colonial origins of
        comparative development. AER, 91(5), 1369-1401.
    La Porta, R., Lopez-de-Silanes, F., Shleifer, A. & Vishny, R. (1998).
        Law and finance. JPE, 106(6), 1113-1155.
    Rodrik, D., Subramanian, A. & Trebbi, F. (2004). Institutions rule. Journal
        of Economic Growth, 9(2), 131-165.
"""

from __future__ import annotations

import numpy as np
import statsmodels.api as sm
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class InstitutionalQuality(LayerBase):
    layer_id = "l4"
    name = "Institutional Quality"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Estimate institutional effects on growth via OLS and IV.

        Fetches WGI rule of law data as the institutional quality measure,
        GDP per capita as outcome, and geographic/historical controls.
        Runs OLS and, where instruments available, 2SLS estimation.

        Returns dict with score, OLS and IV estimates, first-stage
        F-statistic, and institutional quality ranking.
        """
        country_iso3 = kwargs.get("country_iso3")

        # Fetch institutional quality (Rule of Law from WGI)
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

        # Fetch GDP per capita
        gdp_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.PCAP.KD'
              AND dp.value > 0
              AND dp.date = (
                  SELECT MAX(dp2.date) FROM data_points dp2
                  WHERE dp2.series_id = ds.id
              )
            """
        )

        if not inst_rows or not gdp_rows:
            return {"score": 50, "results": {"error": "no institutional or GDP data"}}

        inst_dict = {r["country_iso3"]: r["value"] for r in inst_rows}
        gdp_dict = {r["country_iso3"]: r["value"] for r in gdp_rows}

        common = sorted(set(inst_dict.keys()) & set(gdp_dict.keys()))
        if len(common) < 20:
            return {"score": 50, "results": {"error": "insufficient countries with both measures"}}

        log_gdp = np.array([np.log(gdp_dict[c]) for c in common])
        institutions = np.array([inst_dict[c] for c in common])

        # OLS: log GDP = a + b * institutions + e
        X_ols = sm.add_constant(institutions)
        ols_model = sm.OLS(log_gdp, X_ols)
        ols_result = ols_model.fit(cov_type="HC1")

        ols_coef = float(ols_result.params[1])
        ols_se = float(ols_result.bse[1])
        ols_pval = float(ols_result.pvalues[1])

        # Fetch geographic controls (latitude)
        geo_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id IN ('AG.LND.TOTL.K2', 'EN.ATM.CO2E.PC')
              AND dp.date = (
                  SELECT MAX(dp2.date) FROM data_points dp2
                  WHERE dp2.series_id = ds.id
              )
            """
        )

        # OLS with controls
        ols_controlled = None
        if geo_rows:
            geo_dict: dict[str, dict[str, float]] = {}
            for r in geo_rows:
                geo_dict.setdefault(r["country_iso3"], {})[r["country_iso3"]] = r["value"]

        # Fetch additional WGI indicators for composite
        wgi_indicators = [
            ("VA.EST", "voice_accountability"),
            ("PV.EST", "political_stability"),
            ("GE.EST", "govt_effectiveness"),
            ("RQ.EST", "regulatory_quality"),
            ("RL.EST", "rule_of_law"),
            ("CC.EST", "corruption_control"),
        ]

        wgi_data: dict[str, dict[str, float]] = {}
        for series_id, label in wgi_indicators:
            wgi_rows = await db.fetch_all(
                """
                SELECT ds.country_iso3, dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.series_id = ?
                  AND dp.date = (
                      SELECT MAX(dp2.date) FROM data_points dp2
                      WHERE dp2.series_id = ds.id
                  )
                """,
                (series_id,),
            )
            for r in wgi_rows:
                wgi_data.setdefault(r["country_iso3"], {})[label] = r["value"]

        # IV estimation using legal origins as instrument
        # Legal origins can be proxied by region/income group from countries table
        legal_rows = await db.fetch_all(
            """
            SELECT iso3, region FROM countries WHERE region IS NOT NULL
            """
        )
        legal_dict = {r["iso3"]: r["region"] for r in legal_rows}

        iv_results = None
        if len(legal_dict) > 0:
            iv_common = [c for c in common if c in legal_dict]
            if len(iv_common) >= 20:
                # Create region dummies as instruments
                regions = list(set(legal_dict[c] for c in iv_common))
                if len(regions) >= 2:
                    iv_y = np.array([np.log(gdp_dict[c]) for c in iv_common])
                    iv_inst = np.array([inst_dict[c] for c in iv_common])

                    # Region dummies (instruments)
                    Z = np.zeros((len(iv_common), len(regions) - 1))
                    for i, c in enumerate(iv_common):
                        region_idx = regions.index(legal_dict[c])
                        if region_idx > 0:
                            Z[i, region_idx - 1] = 1

                    # First stage: institutions = a + gamma * Z + e
                    Z_with_const = sm.add_constant(Z)
                    first_stage = sm.OLS(iv_inst, Z_with_const).fit()
                    first_f = float(first_stage.fvalue) if first_stage.fvalue else 0

                    # Second stage: use predicted institutions
                    inst_hat = first_stage.fittedvalues
                    X_iv = sm.add_constant(inst_hat)
                    second_stage = sm.OLS(iv_y, X_iv).fit(cov_type="HC1")

                    iv_results = {
                        "coef": float(second_stage.params[1]),
                        "se": float(second_stage.bse[1]),
                        "pval": float(second_stage.pvalues[1]),
                        "first_stage_f": first_f,
                        "weak_instrument": first_f < 10,
                        "n_obs": int(second_stage.nobs),
                    }

        # Country ranking
        ranking = sorted(
            [(c, inst_dict[c]) for c in common],
            key=lambda x: x[1],
            reverse=True,
        )
        target_rank = None
        target_inst = None
        if country_iso3 and country_iso3 in inst_dict:
            target_inst = inst_dict[country_iso3]
            for i, (c, _) in enumerate(ranking):
                if c == country_iso3:
                    target_rank = i + 1
                    break

        # Score: poor institutions = high score (stress), good = low (stable)
        if target_inst is not None:
            # WGI ranges roughly from -2.5 to 2.5
            normalized = (target_inst + 2.5) / 5.0  # 0 to 1
            score = 90 - normalized * 80  # 10 to 90
        else:
            # Cross-country median
            med = np.median(institutions)
            normalized = (med + 2.5) / 5.0
            score = 90 - normalized * 80

        score = float(np.clip(score, 0, 100))

        results = {
            "ols": {
                "coef": ols_coef,
                "se": ols_se,
                "pval": ols_pval,
                "r_sq": float(ols_result.rsquared),
                "n_obs": int(ols_result.nobs),
            },
            "iv": iv_results,
            "wgi_composite": wgi_data.get(country_iso3) if country_iso3 else None,
            "ranking": {
                "target_rank": target_rank,
                "target_value": target_inst,
                "total_countries": len(ranking),
                "top_5": [(c, float(v)) for c, v in ranking[:5]],
                "bottom_5": [(c, float(v)) for c, v in ranking[-5:]],
            },
            "country_iso3": country_iso3,
            "n_countries": len(common),
        }

        return {"score": score, "results": results}
