"""Addiction Economics module.

Four dimensions of rational addiction and sin-good economics:

1. **Rational addiction** (Becker-Murphy 1988):
   Utility-maximizing agents choose future consumption knowing habits
   form. Characteristic empirical implication: future consumption price
   has a negative effect on current consumption (adjacent complementarity).
   Estimated via Becker-Murphy-Grossman regression:
       ln(c_t) = a0 + a1*ln(c_{t-1}) + a2*ln(c_{t+1}) + a3*ln(p_t) + e_t
   Instrumental variables: past/future prices as instruments.

2. **Sin tax effectiveness** (Chaloupka 1991, Gruber & Koszegi 2001):
   Pigou-optimal tax corrects externality + internality. Gruber-Koszegi
   extend rational addiction to include self-control failures: consumers
   regret current choices, justifying higher taxes than pure externality.
   Estimated via price-elasticity regression.

3. **Price elasticity of addictive goods**:
   Meta-analysis ranges: cigarettes -0.3 to -0.5, alcohol -0.5,
   gambling -0.9. Short-run elasticity smaller than long-run (Becker-Murphy:
   long-run elasticity = 2-3x short-run due to habit stock).

4. **Social cost externalities**:
   Healthcare costs, productivity losses, crime, accidents. CDC estimates
   $300B/year for alcohol, $240B for tobacco in the US. Measured as
   externality cost as % of GDP or per-capita terms.

Score: high consumption levels + low price elasticity response + large
social cost externalities -> high stress.

References:
    Becker, G. & Murphy, K. (1988). "A Theory of Rational Addiction."
        JPE 96(4).
    Gruber, J. & Koszegi, B. (2001). "Is Addiction 'Rational'? Theory
        and Evidence." QJE 116(4).
    Chaloupka, F. (1991). "Rational Addictive Behavior and Cigarette
        Smoking." JPE 99(4).
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class AddictionEconomics(LayerBase):
    layer_id = "l13"
    name = "Addiction Economics"

    async def compute(self, db, **kwargs) -> dict:
        """Estimate rational addiction parameters and social cost externalities.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 code (default USA)
        """
        country = kwargs.get("country_iso3", "USA")

        # Consumption data for addictive goods (tobacco, alcohol)
        consumption_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name, ds.series_id
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%tobacco%consumption%' OR ds.name LIKE '%cigarette%consumption%'
                   OR ds.name LIKE '%alcohol%consumption%' OR ds.name LIKE '%smoking%prevalence%'
                   OR ds.name LIKE '%alcohol%use%disorder%' OR ds.name LIKE '%substance%use%')
            ORDER BY dp.date
            """,
            (country,),
        )

        # Price indices for sin goods
        price_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name, ds.series_id
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source IN ('fred', 'bls', 'wdi')
              AND (ds.name LIKE '%tobacco%price%' OR ds.name LIKE '%alcohol%price%'
                   OR ds.name LIKE '%cigarette%price%' OR ds.name LIKE '%excise%tobacco%'
                   OR ds.name LIKE '%sin%tax%')
            ORDER BY dp.date
            """,
            (country,),
        )

        # Social cost / health expenditure data
        social_cost_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source IN ('wdi', 'who', 'fred')
              AND (ds.name LIKE '%health%expenditure%gdp%' OR ds.name LIKE '%disease%burden%'
                   OR ds.name LIKE '%mortality%noncommunicable%' OR ds.name LIKE '%out%of%pocket%health%'
                   OR ds.name LIKE '%health%cost%gdp%')
            ORDER BY dp.date
            """,
            (country,),
        )

        # Tax revenue from sin goods
        tax_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%excise%revenue%' OR ds.name LIKE '%sin%tax%revenue%'
                   OR ds.name LIKE '%tobacco%tax%' OR ds.name LIKE '%alcohol%tax%revenue%')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not consumption_rows and not social_cost_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no addiction/substance use data"}

        # --- 1. Consumption level analysis ---
        consumption_analysis = None
        consumption_stress = 0.5
        if consumption_rows:
            # Group by series_id
            series_map: dict[str, list] = {}
            for r in consumption_rows:
                series_map.setdefault(r["series_id"], []).append(
                    (r["date"], float(r["value"]))
                )

            # Use the longest series
            primary_sid = max(series_map, key=lambda s: len(series_map[s]))
            primary_data = sorted(series_map[primary_sid], key=lambda x: x[0])
            dates = [d for d, _ in primary_data]
            vals = np.array([v for _, v in primary_data])
            latest_val = float(vals[-1])

            # Normalize: prevalence rates typically 0-100% or per 100k
            if np.max(vals) > 100:
                # Per 100k or absolute -> normalize by percentile
                consumption_stress = float(np.clip(latest_val / np.max(vals), 0, 1))
            else:
                # Percent
                consumption_stress = float(np.clip(latest_val / 100.0, 0, 1))

            # Becker-Murphy adjacent complementarity test
            bm_result = None
            if len(vals) >= 10:
                bm_result = self._becker_murphy_test(vals)

            consumption_analysis = {
                "latest_prevalence": round(latest_val, 3),
                "mean_prevalence": round(float(np.mean(vals)), 3),
                "consumption_stress": round(consumption_stress, 4),
                "n_obs": len(vals),
                "date_range": [str(dates[0]), str(dates[-1])],
                "series": primary_sid,
            }
            if bm_result:
                consumption_analysis["becker_murphy_adjacent_complementarity"] = bm_result

        # --- 2. Price elasticity estimation ---
        elasticity_result = None
        short_run_elasticity = -0.4  # Default meta-analytic value
        if consumption_rows and price_rows and len(consumption_rows) >= 8 and len(price_rows) >= 8:
            cons_map = {str(r["date"])[:4]: float(r["value"]) for r in consumption_rows}
            price_map = {str(r["date"])[:4]: float(r["value"]) for r in price_rows}
            common = sorted(set(cons_map.keys()) & set(price_map.keys()))

            if len(common) >= 8:
                c_arr = np.log(np.array([cons_map[y] for y in common]) + 1e-10)
                p_arr = np.log(np.array([price_map[y] for y in common]) + 1e-10)
                slope, _, r_val, p_val, _ = stats.linregress(p_arr, c_arr)
                short_run_elasticity = float(slope)

                # Long-run elasticity (Becker-Murphy: LR ~ 2-3x SR)
                lr_elasticity = short_run_elasticity * 2.5

                elasticity_result = {
                    "short_run_price_elasticity": round(short_run_elasticity, 4),
                    "long_run_elasticity_estimate": round(lr_elasticity, 4),
                    "r_squared": round(float(r_val ** 2), 4),
                    "p_value": round(float(p_val), 4),
                    "n_obs": len(common),
                    "chaloupka_consistent": short_run_elasticity < 0,
                    "meta_benchmark": "Chaloupka 1991 SR: -0.4; Becker-Murphy LR: ~2.5x SR",
                    "sin_tax_effectiveness": "high" if abs(short_run_elasticity) > 0.5 else "moderate",
                }

        # --- 3. Social cost externalities ---
        social_cost_analysis = None
        social_cost_stress = 0.4
        if social_cost_rows:
            sv = np.array([float(r["value"]) for r in social_cost_rows])
            sc_dates = [r["date"] for r in social_cost_rows]
            latest_sc = float(sv[-1])

            # Health expenditure % GDP: WHO global average ~10%. Addiction-related = subset.
            # Stress based on level relative to 10% benchmark
            if latest_sc > 1:
                sc_pct = latest_sc  # Already percentage
            else:
                sc_pct = latest_sc * 100.0

            # Higher health expenditure % GDP partially reflects social cost burden
            social_cost_stress = float(np.clip((sc_pct - 5.0) / 15.0, 0, 1))

            social_cost_analysis = {
                "latest_health_exp_gdp_pct": round(sc_pct, 2),
                "mean_health_exp_gdp_pct": round(
                    float(np.mean(sv)) if latest_sc > 1 else float(np.mean(sv)) * 100, 2
                ),
                "social_cost_stress": round(social_cost_stress, 4),
                "n_obs": len(sv),
                "date_range": [str(sc_dates[0]), str(sc_dates[-1])],
                "note": "Health expenditure as proxy for addiction-related social cost burden",
                "reference": "CDC: $300B alcohol/year, $240B tobacco/year (US); Gruber & Koszegi 2001",
            }

        # --- 4. Sin tax revenue ---
        tax_analysis = None
        if tax_rows:
            tv = np.array([float(r["value"]) for r in tax_rows])
            tax_dates = [r["date"] for r in tax_rows]
            latest_tax = float(tv[-1])

            if len(tv) >= 3:
                t = np.arange(len(tv), dtype=float)
                slope, _, r_val, _, _ = stats.linregress(t, tv)
                trend_dir = "increasing" if slope > 0 else "decreasing"
            else:
                trend_dir = "unknown"

            tax_analysis = {
                "latest_value": round(latest_tax, 3),
                "mean_value": round(float(np.mean(tv)), 3),
                "trend_direction": trend_dir,
                "n_obs": len(tv),
                "date_range": [str(tax_dates[0]), str(tax_dates[-1])],
                "reference": "Pigou optimal tax; Gruber-Koszegi internality correction",
            }

        # --- Score ---
        # Components: consumption 35, social cost 35, elasticity-based tax ineffectiveness 30
        elasticity_ineffectiveness = float(np.clip(1.0 - abs(short_run_elasticity) / 1.0, 0, 1))
        score = float(np.clip(
            consumption_stress * 35.0
            + social_cost_stress * 35.0
            + elasticity_ineffectiveness * 30.0,
            0, 100,
        ))

        result = {
            "score": round(score, 2),
            "country": country,
            "score_components": {
                "consumption_level": round(consumption_stress * 35.0, 2),
                "social_cost_burden": round(social_cost_stress * 35.0, 2),
                "tax_ineffectiveness": round(elasticity_ineffectiveness * 30.0, 2),
            },
        }

        if consumption_analysis:
            result["consumption_analysis"] = consumption_analysis
        if elasticity_result:
            result["price_elasticity"] = elasticity_result
        if social_cost_analysis:
            result["social_cost_externalities"] = social_cost_analysis
        if tax_analysis:
            result["sin_tax_revenue"] = tax_analysis

        return result

    @staticmethod
    def _becker_murphy_test(vals: np.ndarray) -> dict:
        """Test Becker-Murphy adjacent complementarity.

        Adjacent complementarity: past consumption raises marginal utility
        of current consumption. In time series, implies positive autocorrelation
        structure with persistence beyond simple AR(1).

        Regression: c_t = a0 + a1*c_{t-1} + a2*c_{t+2} + e_t
        If a2 > 0 (future consumption positively predicts current), this
        supports the rational addiction forward-looking structure.
        """
        n = len(vals)
        if n < 10:
            return {"note": "insufficient data for Becker-Murphy test"}

        # c_{t}: index 1 to n-2
        # c_{t-1}: index 0 to n-3
        # c_{t+1}: index 2 to n-1 (adjacent forward)
        c_t = vals[1:-1]
        c_lag = vals[:-2]
        c_lead = vals[2:]

        X = np.column_stack([np.ones(len(c_t)), c_lag, c_lead])
        beta = np.linalg.lstsq(X, c_t, rcond=None)[0]

        predicted = X @ beta
        ss_res = float(np.sum((c_t - predicted) ** 2))
        ss_tot = float(np.sum((c_t - np.mean(c_t)) ** 2))
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

        return {
            "lag_coefficient": round(float(beta[1]), 4),
            "lead_coefficient": round(float(beta[2]), 4),
            "r_squared": round(r2, 4),
            "adjacent_complementarity": float(beta[2]) > 0,
            "rational_addiction_support": float(beta[1]) > 0 and float(beta[2]) > 0,
            "note": "Becker-Murphy: positive lead coefficient supports forward-looking rational addiction",
            "n_obs": int(len(c_t)),
        }
