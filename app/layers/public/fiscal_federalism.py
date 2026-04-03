"""Fiscal federalism and intergovernmental finance.

Implements core fiscal federalism diagnostics:

1. Vertical fiscal imbalance (VFI): mismatch between subnational
   expenditure responsibilities and own-revenue capacity.
       VFI = 1 - (own_revenue / expenditure)
   VFI near 1 means heavy dependence on transfers.

2. Equalization transfers: measures whether intergovernmental transfers
   reduce fiscal disparities across jurisdictions. Uses coefficient of
   variation reduction and the equalization coefficient:
       EQ = 1 - CV(post_transfer) / CV(pre_transfer)

3. Tiebout (1956) sorting: tests whether jurisdictions differentiate in
   tax/service bundles. Measured by between-jurisdiction variance in
   tax rates and service levels relative to within-jurisdiction variance.
   High between/within ratio suggests Tiebout sorting is operative.

4. Oates (1972) decentralization theorem: decentralization welfare gains
   arise when preferences are heterogeneous across jurisdictions and
   spillovers are small. Tests whether expenditure decentralization
   correlates with better outcomes (growth, service delivery).

References:
    Tiebout, C. (1956). A Pure Theory of Local Expenditures. JPE, 64(5).
    Oates, W. (1972). Fiscal Federalism. Harcourt Brace.
    Boadway, R. & Shah, A. (2009). Fiscal Federalism. Cambridge.
    Martinez-Vazquez, J. & Timofeev, A. (2010). Decentralization Measures
        Revisited. Public Finance and Management, 10(1), 13-47.

Sources: WDI, IMF GFS (government finance statistics)
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class FiscalFederalism(LayerBase):
    layer_id = "l10"
    name = "Fiscal Federalism"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        results = {"country": country}

        # --- Vertical fiscal imbalance ---
        sub_rev_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('SUBNATIONAL_OWN_REV', 'GC.REV.XGRT.GD.ZS')
            ORDER BY dp.date
            """,
            (country,),
        )

        sub_exp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('SUBNATIONAL_EXP', 'GC.XPN.TOTL.GD.ZS')
            ORDER BY dp.date
            """,
            (country,),
        )

        if sub_rev_rows and sub_exp_rows:
            rev_dict = {r["date"]: float(r["value"]) for r in sub_rev_rows}
            exp_dict = {r["date"]: float(r["value"]) for r in sub_exp_rows}
            common = sorted(set(rev_dict) & set(exp_dict))

            if common:
                latest = common[-1]
                own_rev = rev_dict[latest]
                expenditure = exp_dict[latest]
                vfi = 1.0 - (own_rev / expenditure) if expenditure > 0 else 0.0

                # VFI time series
                vfi_series = []
                for d in common[-10:]:
                    e = exp_dict[d]
                    r = rev_dict[d]
                    v = 1.0 - (r / e) if e > 0 else 0.0
                    vfi_series.append({"date": d, "vfi": round(v, 4)})

                results["vertical_fiscal_imbalance"] = {
                    "vfi": round(vfi, 4),
                    "own_revenue_pct_gdp": round(own_rev, 2),
                    "expenditure_pct_gdp": round(expenditure, 2),
                    "transfer_dependence": "high" if vfi > 0.5 else "moderate" if vfi > 0.3 else "low",
                    "trend": vfi_series,
                }
            else:
                results["vertical_fiscal_imbalance"] = {"error": "no overlapping dates"}
        else:
            results["vertical_fiscal_imbalance"] = {"error": "insufficient fiscal data"}

        # --- Equalization transfers ---
        # Fetch subnational fiscal capacity data across jurisdictions
        jurisdiction_rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id LIKE '%JURISDICTION_FISCAL%'
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if jurisdiction_rows and len(jurisdiction_rows) >= 5:
            import json

            pre_transfer = []
            post_transfer = []
            for r in jurisdiction_rows:
                meta = json.loads(r["metadata"]) if r.get("metadata") else {}
                pre = meta.get("pre_transfer_capacity")
                post = meta.get("post_transfer_capacity")
                if pre is not None and post is not None:
                    pre_transfer.append(float(pre))
                    post_transfer.append(float(post))

            if len(pre_transfer) >= 5:
                pre_arr = np.array(pre_transfer)
                post_arr = np.array(post_transfer)

                cv_pre = float(np.std(pre_arr) / np.mean(pre_arr)) if np.mean(pre_arr) > 0 else 0
                cv_post = float(np.std(post_arr) / np.mean(post_arr)) if np.mean(post_arr) > 0 else 0
                eq_coeff = 1.0 - (cv_post / cv_pre) if cv_pre > 0 else 0

                results["equalization"] = {
                    "cv_pre_transfer": round(cv_pre, 4),
                    "cv_post_transfer": round(cv_post, 4),
                    "equalization_coefficient": round(eq_coeff, 4),
                    "n_jurisdictions": len(pre_transfer),
                    "effectiveness": (
                        "strong" if eq_coeff > 0.5
                        else "moderate" if eq_coeff > 0.2
                        else "weak" if eq_coeff > 0
                        else "disequalizing"
                    ),
                }
            else:
                results["equalization"] = {"error": "insufficient jurisdiction data"}
        else:
            results["equalization"] = {"error": "no jurisdiction fiscal data"}

        # --- Tiebout sorting ---
        # Test between-jurisdiction variance vs within-jurisdiction variance
        # in tax rates / public service spending
        tiebout_rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id LIKE '%LOCAL_TAX_RATE%'
            """,
            (country,),
        )

        if tiebout_rows and len(tiebout_rows) >= 10:
            import json

            groups: dict[str, list[float]] = {}
            for r in tiebout_rows:
                meta = json.loads(r["metadata"]) if r.get("metadata") else {}
                region = meta.get("region", "unknown")
                groups.setdefault(region, []).append(float(r["value"]))

            if len(groups) >= 3:
                group_list = [np.array(v) for v in groups.values() if len(v) >= 2]
                if len(group_list) >= 3:
                    f_stat, p_value = sp_stats.f_oneway(*group_list)

                    grand_mean = np.mean(np.concatenate(group_list))
                    between_var = np.var([np.mean(g) for g in group_list])
                    within_var = np.mean([np.var(g) for g in group_list])
                    f_ratio = between_var / within_var if within_var > 0 else 0

                    results["tiebout_sorting"] = {
                        "f_statistic": round(float(f_stat), 3),
                        "p_value": round(float(p_value), 4),
                        "between_variance": round(float(between_var), 4),
                        "within_variance": round(float(within_var), 4),
                        "variance_ratio": round(f_ratio, 3),
                        "n_regions": len(group_list),
                        "grand_mean_rate": round(float(grand_mean), 4),
                        "sorting_evidence": "strong" if p_value < 0.01 else "moderate" if p_value < 0.05 else "weak",
                    }
                else:
                    results["tiebout_sorting"] = {"error": "too few regions with data"}
            else:
                results["tiebout_sorting"] = {"error": "insufficient regional variation"}
        else:
            results["tiebout_sorting"] = {"error": "no local tax rate data"}

        # --- Oates decentralization theorem test ---
        # Cross-country: correlate expenditure decentralization with growth/outcomes
        decentr_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id IN ('EXPENDITURE_DECENTRALIZATION', 'GC.XPN.TOTL.GD.ZS')
              AND dp.value > 0
            ORDER BY dp.date DESC
            """
        )

        growth_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id IN ('NY.GDP.PCAP.KD.ZG', 'GDPPC_GROWTH')
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """
        )

        if decentr_rows and growth_rows:
            dec_latest: dict[str, float] = {}
            for r in decentr_rows:
                iso = r["country_iso3"]
                if iso not in dec_latest:
                    dec_latest[iso] = float(r["value"])

            gr_latest: dict[str, float] = {}
            for r in growth_rows:
                iso = r["country_iso3"]
                if iso not in gr_latest:
                    gr_latest[iso] = float(r["value"])

            common_iso = sorted(set(dec_latest) & set(gr_latest))
            if len(common_iso) >= 15:
                x = np.array([dec_latest[c] for c in common_iso])
                y = np.array([gr_latest[c] for c in common_iso])

                slope, intercept, r_value, p_value, std_err = sp_stats.linregress(x, y)

                results["oates_test"] = {
                    "n_countries": len(common_iso),
                    "slope": round(float(slope), 4),
                    "r_squared": round(float(r_value ** 2), 4),
                    "p_value": round(float(p_value), 4),
                    "interpretation": (
                        "decentralization associated with higher growth"
                        if slope > 0 and p_value < 0.05
                        else "decentralization associated with lower growth"
                        if slope < 0 and p_value < 0.05
                        else "no significant relationship"
                    ),
                }
            else:
                results["oates_test"] = {"error": "insufficient cross-country data"}
        else:
            results["oates_test"] = {"error": "no decentralization or growth data"}

        # --- Score ---
        score = 30.0

        # VFI: high transfer dependence
        vfi_data = results.get("vertical_fiscal_imbalance", {})
        vfi_val = vfi_data.get("vfi")
        if vfi_val is not None:
            if vfi_val > 0.6:
                score += 25
            elif vfi_val > 0.4:
                score += 15
            elif vfi_val > 0.2:
                score += 5

        # Weak equalization
        eq_data = results.get("equalization", {})
        eff = eq_data.get("effectiveness")
        if eff == "disequalizing":
            score += 20
        elif eff == "weak":
            score += 10

        # Oates: negative relationship
        oates = results.get("oates_test", {})
        if oates.get("slope") is not None and oates["slope"] < 0 and oates.get("p_value", 1) < 0.05:
            score += 10

        score = max(0.0, min(100.0, score))

        return {"score": round(score, 1), "results": results}
