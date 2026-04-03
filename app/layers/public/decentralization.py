"""Fiscal decentralization and subnational governance quality.

Key frameworks:

1. Fiscal decentralization index (Oates 1999, Martinez-Vazquez 2003):
   FDI = subnational_expenditure / total_government_expenditure
   Revenue decentralization = subnational_own_revenue / total_revenue
   Asymmetry = FDI_expenditure - FDI_revenue  (positive = transfer dependence)

2. Local revenue autonomy (Bird & Smart 2002):
   Autonomy = (own_tax + own_non_tax) / total_subnational_revenue
   High autonomy -> Tiebout competition, accountability, hard budget constraints.
   Low autonomy -> soft constraints, moral hazard, over-borrowing.

3. Subnational spending efficiency (DEA-inspired):
   Efficiency = output_index / expenditure_per_capita
   Output index: composite of education attainment, health outcomes,
   infrastructure quality at subnational level.
   Cross-jurisdiction variation reveals best-practice frontier.

4. Service delivery quality (World Bank PETS methodology):
   Expenditure tracking: share of resources reaching frontline facilities.
   Leakage = 1 - (facility_receipts / central_transfer)
   Quality score = composite of access, reliability, satisfaction.

References:
    Oates, W. (1999). An Essay on Fiscal Federalism. JEL 37(3).
    Bird, R. & Smart, M. (2002). Intergovernmental Fiscal Transfers. World Dev.
    Martinez-Vazquez, J. & Timofeev, A. (2010). Decentralization Measures Revisited.
    Boadway, R. & Shah, A. (2009). Fiscal Federalism. Cambridge UP.
    World Bank (2004). Public Expenditure Tracking Surveys. PETS Toolkit.

Sources: IMF GFS, World Bank WDI, OECD Fiscal Decentralization Database.
"""

from __future__ import annotations

import json

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class Decentralization(LayerBase):
    layer_id = "l10"
    name = "Decentralization"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")
        results: dict = {"country": country}

        # --- Fiscal decentralization index ---
        subnational_exp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('SUBNATIONAL_EXP_SHARE', 'GC.XPN.TOTL.GD.ZS')
            ORDER BY dp.date
            """,
            (country,),
        )

        total_exp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('TOTAL_GOV_EXP_GDP', 'GC.XPN.TOTL.GD.ZS')
            ORDER BY dp.date
            """,
            (country,),
        )

        fdi_result = None
        if subnational_exp_rows and total_exp_rows:
            sub_dict = {r["date"]: float(r["value"]) for r in subnational_exp_rows}
            tot_dict = {r["date"]: float(r["value"]) for r in total_exp_rows}
            common = sorted(set(sub_dict) & set(tot_dict))
            if common:
                latest = common[-1]
                sub_exp = sub_dict[latest]
                tot_exp = tot_dict[latest]
                fdi = sub_exp / tot_exp if tot_exp > 0 else 0.0

                fdi_series = [
                    {"date": d, "fdi": round(sub_dict[d] / tot_dict[d], 4) if tot_dict[d] > 0 else None}
                    for d in common[-10:]
                    if tot_dict[d] > 0
                ]

                fdi_result = {
                    "fiscal_decentralization_index": round(fdi, 4),
                    "subnational_exp_pct_total": round(sub_exp, 2),
                    "total_gov_exp_pct_gdp": round(tot_exp, 2),
                    "trend": fdi_series,
                    "classification": (
                        "highly_decentralized" if fdi > 0.40
                        else "moderately_decentralized" if fdi > 0.25
                        else "centralized"
                    ),
                }
        results["fiscal_decentralization"] = fdi_result or {"error": "insufficient expenditure data"}

        # --- Local revenue autonomy ---
        own_rev_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('SUBNATIONAL_OWN_REVENUE', 'LOCAL_TAX_REVENUE')
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        total_subnational_rev_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('SUBNATIONAL_TOTAL_REVENUE', 'LOCAL_TOTAL_REVENUE')
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        autonomy_result = None
        if own_rev_rows and total_subnational_rev_rows:
            own_dict = {r["date"]: float(r["value"]) for r in own_rev_rows}
            tot_sub_dict = {r["date"]: float(r["value"]) for r in total_subnational_rev_rows}
            common_rev = sorted(set(own_dict) & set(tot_sub_dict))
            if common_rev:
                latest_rev = common_rev[-1]
                own = own_dict[latest_rev]
                tot_sub = tot_sub_dict[latest_rev]
                autonomy = own / tot_sub if tot_sub > 0 else 0.0
                autonomy_result = {
                    "local_revenue_autonomy": round(autonomy, 4),
                    "own_revenue_pct": round(own, 2),
                    "total_subnational_revenue_pct": round(tot_sub, 2),
                    "transfer_dependence": round(1.0 - autonomy, 4),
                    "hard_budget_constraint": autonomy > 0.5,
                    "date": latest_rev,
                }
        results["local_revenue_autonomy"] = autonomy_result or {"error": "no local revenue data"}

        # --- Subnational spending efficiency ---
        efficiency_rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id LIKE '%SUBNATIONAL_EFFICIENCY%'
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        efficiency_result = None
        if efficiency_rows and len(efficiency_rows) >= 5:
            outputs = []
            expenditures = []
            for r in efficiency_rows:
                meta = json.loads(r["metadata"]) if r.get("metadata") else {}
                out = meta.get("output_index")
                exp_pc = meta.get("expenditure_per_capita")
                if out is not None and exp_pc is not None and exp_pc > 0:
                    outputs.append(float(out))
                    expenditures.append(float(exp_pc))

            if len(outputs) >= 5:
                outputs_arr = np.array(outputs)
                exp_arr = np.array(expenditures)
                efficiency_scores = outputs_arr / (exp_arr / np.mean(exp_arr))

                mean_eff = float(np.mean(efficiency_scores))
                cv_eff = float(np.std(efficiency_scores) / mean_eff) if mean_eff > 0 else 0.0
                frontier_eff = float(np.max(efficiency_scores))

                # Regression: output on expenditure
                slope, intercept, r_val, p_val, _ = sp_stats.linregress(exp_arr, outputs_arr)
                efficiency_result = {
                    "mean_efficiency_score": round(mean_eff, 4),
                    "cv_efficiency": round(cv_eff, 4),
                    "frontier_efficiency": round(frontier_eff, 4),
                    "n_jurisdictions": len(outputs),
                    "exp_output_slope": round(float(slope), 4),
                    "r_squared": round(float(r_val ** 2), 4),
                    "p_value": round(float(p_val), 4),
                    "high_dispersion": cv_eff > 0.3,
                }
        results["spending_efficiency"] = efficiency_result or {"error": "no subnational efficiency data"}

        # --- Service delivery quality ---
        service_rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('SERVICE_DELIVERY_INDEX', 'LOCAL_SERVICE_QUALITY')
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )

        service_result = None
        if service_rows:
            meta = json.loads(service_rows[0]["metadata"]) if service_rows[0].get("metadata") else {}
            composite_score = float(service_rows[0]["value"])
            leakage = meta.get("expenditure_leakage", None)
            access_score = meta.get("access_score", None)
            satisfaction = meta.get("citizen_satisfaction", None)

            service_result = {
                "composite_service_quality": round(composite_score, 2),
                "expenditure_leakage": round(float(leakage), 4) if leakage is not None else None,
                "access_score": round(float(access_score), 2) if access_score is not None else None,
                "citizen_satisfaction": round(float(satisfaction), 2) if satisfaction is not None else None,
            }
        results["service_delivery"] = service_result or {"error": "no service delivery data"}

        # --- Score ---
        score = 25.0

        # Low autonomy -> more stress
        if autonomy_result and not autonomy_result.get("error"):
            aut = autonomy_result["local_revenue_autonomy"]
            if aut < 0.2:
                score += 25.0
            elif aut < 0.35:
                score += 15.0
            elif aut < 0.5:
                score += 8.0

        # High CV in spending efficiency -> stress
        if efficiency_result and not efficiency_result.get("error"):
            cv = efficiency_result["cv_efficiency"]
            score += min(20.0, cv * 50.0)

        # Low FDI with high asymmetry
        if fdi_result and not fdi_result.get("error"):
            if fdi_result["classification"] == "centralized":
                score += 10.0

        # Low service quality
        if service_result and not service_result.get("error"):
            sq = service_result["composite_service_quality"]
            if sq < 40:
                score += 20.0
            elif sq < 60:
                score += 10.0
            elif sq < 75:
                score += 5.0

        score = float(np.clip(score, 0.0, 100.0))
        return {"score": round(score, 1), "results": results}
