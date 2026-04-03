"""Regulatory impact assessment (RIA): cost-benefit methodology, compliance
costs, small business impacts, and regulatory quality indicators.

Key frameworks:

1. RIA cost-benefit methodology (OECD 2020):
   Net benefit = PV(benefits) - PV(costs) over regulatory horizon.
   Benefit categories: safety, health, environment, economic efficiency.
   Cost categories: compliance costs, administrative burden, enforcement costs.
   NPV > 0 and BCR > 1 required for regulatory approval.

2. Compliance cost estimation (Standard Cost Model, EC 2004):
   Administrative burden = Price * Quantity * Frequency
   where Price = hourly_cost * time_per_activity
         Quantity = number of affected businesses
         Frequency = times per year the obligation applies
   Baseline administrative burden as % of GDP benchmarked against OECD.

3. Small business impact (SBI) assessment:
   SME disproportionality ratio = compliance_cost_SME/revenue vs large_firm.
   If ratio > 2: disproportionate burden.
   One-In-One-Out / One-In-Two-Out rules: net regulatory cost reduction target.

4. Regulatory quality indicators (World Bank Doing Business; OECD PMR):
   Product market regulation (PMR) index: barriers to entry, state control,
   trade/investment barriers. Lower PMR -> better regulatory quality.
   World Governance Indicator: Rule of Law, Regulatory Quality.

References:
    OECD (2020). Regulatory Impact Assessment. OECD Best Practice Principles.
    European Commission (2004). Standard Cost Model Manual.
    World Bank (2019). Doing Business Methodology Note.
    Conway, P. & Nicoletti, G. (2006). Product Market Regulation in OECD Countries.
        OECD Economics Department Working Papers, No. 530.
    OECD (2018). OECD PMR Indicators. OECD Economics Department WP No. 1495.

Sources: World Bank Doing Business, OECD PMR, World Bank WGI.
"""

from __future__ import annotations

import json

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class RegulatoryImpact(LayerBase):
    layer_id = "l10"
    name = "Regulatory Impact"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")
        results: dict = {"country": country}

        # --- RIA quality and adoption ---
        ria_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('RIA_QUALITY_INDEX', 'REGULATORY_QUALITY_WGI')
            ORDER BY dp.date
            """,
            (country,),
        )

        ria_result = None
        if ria_rows:
            latest = ria_rows[-1]
            quality_score = float(latest["value"])
            meta = json.loads(latest["metadata"]) if latest.get("metadata") else {}
            adoption_rate = meta.get("ria_adoption_rate", None)
            cba_quality = meta.get("cba_quality_score", None)
            consultation_score = meta.get("consultation_score", None)

            if len(ria_rows) >= 3:
                vals_arr = np.array([float(r["value"]) for r in ria_rows[-10:]])
                t_arr = np.arange(len(vals_arr))
                slope_ria, _, _, p_ria, _ = sp_stats.linregress(t_arr, vals_arr)
            else:
                slope_ria, p_ria = None, None

            ria_result = {
                "regulatory_quality_score": round(quality_score, 2),
                "ria_adoption_rate": round(float(adoption_rate), 4) if adoption_rate is not None else None,
                "cba_quality": round(float(cba_quality), 2) if cba_quality is not None else None,
                "consultation_score": round(float(consultation_score), 2) if consultation_score is not None else None,
                "trend_slope": round(float(slope_ria), 4) if slope_ria is not None else None,
                "improving": slope_ria > 0 if slope_ria is not None else None,
                "rating": (
                    "excellent" if quality_score > 1.0
                    else "good" if quality_score > 0.5
                    else "moderate" if quality_score > -0.5
                    else "weak"
                ),
            }
        results["ria_quality"] = ria_result or {"error": "no RIA quality data"}

        # --- Compliance cost estimation (SCM) ---
        compliance_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('COMPLIANCE_COST_GDP', 'ADMIN_BURDEN_GDP')
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )

        compliance_result = None
        if compliance_rows:
            meta = json.loads(compliance_rows[0]["metadata"]) if compliance_rows[0].get("metadata") else {}
            admin_burden_pct_gdp = float(compliance_rows[0]["value"])
            total_compliance = meta.get("total_compliance_cost_pct_gdp", admin_burden_pct_gdp * 1.5)
            oecd_avg = meta.get("oecd_average_burden", 3.5)
            hours_per_firm = meta.get("annual_hours_per_firm", None)

            compliance_result = {
                "admin_burden_pct_gdp": round(admin_burden_pct_gdp, 2),
                "total_compliance_cost_pct_gdp": round(float(total_compliance), 2),
                "oecd_average_pct_gdp": round(float(oecd_avg), 2),
                "relative_to_oecd": round(admin_burden_pct_gdp / float(oecd_avg), 3) if float(oecd_avg) > 0 else None,
                "annual_hours_per_firm": round(float(hours_per_firm), 0) if hours_per_firm is not None else None,
                "above_oecd_average": admin_burden_pct_gdp > float(oecd_avg),
            }
        results["compliance_cost"] = compliance_result or {"error": "no compliance cost data"}

        # --- Small business impact ---
        sbi_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('SME_COMPLIANCE_RATIO', 'SMALL_BIZ_BURDEN')
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )

        sbi_result = None
        if sbi_rows:
            meta = json.loads(sbi_rows[0]["metadata"]) if sbi_rows[0].get("metadata") else {}
            sme_cost_pct_revenue = float(sbi_rows[0]["value"])
            large_firm_cost_pct = meta.get("large_firm_compliance_pct_revenue", None)

            disproportionality = None
            if large_firm_cost_pct and float(large_firm_cost_pct) > 0:
                disproportionality = sme_cost_pct_revenue / float(large_firm_cost_pct)

            oioo_target = meta.get("one_in_one_out_net_cost", None)

            sbi_result = {
                "sme_compliance_pct_revenue": round(sme_cost_pct_revenue, 2),
                "large_firm_compliance_pct_revenue": round(float(large_firm_cost_pct), 2) if large_firm_cost_pct is not None else None,
                "disproportionality_ratio": round(disproportionality, 3) if disproportionality is not None else None,
                "disproportionate_burden": disproportionality > 2.0 if disproportionality is not None else None,
                "oioo_net_cost_pct_gdp": round(float(oioo_target), 3) if oioo_target is not None else None,
            }
        results["small_business_impact"] = sbi_result or {"error": "no SME impact data"}

        # --- Product market regulation (PMR) ---
        pmr_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('OECD_PMR_INDEX', 'PRODUCT_MARKET_REGULATION')
            ORDER BY dp.date
            """,
            (country,),
        )

        pmr_result = None
        if pmr_rows:
            pmr_latest = pmr_rows[-1]
            pmr_val = float(pmr_latest["value"])
            meta = json.loads(pmr_latest["metadata"]) if pmr_latest.get("metadata") else {}

            state_control = meta.get("state_control_sub_index", None)
            barriers_entry = meta.get("barriers_entry_sub_index", None)
            trade_barriers = meta.get("trade_investment_barriers_sub_index", None)

            pmr_series = [
                {"date": r["date"], "pmr": round(float(r["value"]), 4)}
                for r in pmr_rows[-8:]
            ]

            if len(pmr_rows) >= 3:
                pmr_arr = np.array([float(r["value"]) for r in pmr_rows[-8:]])
                t_arr = np.arange(len(pmr_arr))
                slope_pmr, _, _, _, _ = sp_stats.linregress(t_arr, pmr_arr)
            else:
                slope_pmr = None

            pmr_result = {
                "pmr_index": round(pmr_val, 3),
                "state_control": round(float(state_control), 3) if state_control is not None else None,
                "barriers_to_entry": round(float(barriers_entry), 3) if barriers_entry is not None else None,
                "trade_investment_barriers": round(float(trade_barriers), 3) if trade_barriers is not None else None,
                "trend": pmr_series,
                "trend_slope": round(float(slope_pmr), 4) if slope_pmr is not None else None,
                "liberalizing": slope_pmr < 0 if slope_pmr is not None else None,
                "oecd_percentile": meta.get("oecd_percentile", None),
                "rating": (
                    "low_regulation" if pmr_val < 1.5
                    else "moderate" if pmr_val < 2.5
                    else "restrictive"
                ),
            }
        results["product_market_regulation"] = pmr_result or {"error": "no PMR data"}

        # --- Score ---
        score = 25.0

        # Poor RIA quality
        if ria_result and not ria_result.get("error"):
            rq = ria_result["regulatory_quality_score"]
            if rq < -0.5:
                score += 25.0
            elif rq < 0:
                score += 15.0
            elif rq < 0.5:
                score += 7.0

        # High compliance cost
        if compliance_result and not compliance_result.get("error"):
            if compliance_result.get("above_oecd_average"):
                rel = compliance_result.get("relative_to_oecd", 1.0) or 1.0
                score += min(20.0, (rel - 1.0) * 15.0)

        # SME disproportionality
        if sbi_result and not sbi_result.get("error"):
            if sbi_result.get("disproportionate_burden"):
                ratio = sbi_result.get("disproportionality_ratio", 1.0) or 1.0
                score += min(15.0, (ratio - 2.0) * 5.0)

        # Restrictive PMR
        if pmr_result and not pmr_result.get("error"):
            if pmr_result["rating"] == "restrictive":
                score += 15.0
            elif pmr_result["rating"] == "moderate":
                score += 7.0

        score = float(np.clip(score, 0.0, 100.0))
        return {"score": round(score, 1), "results": results}
