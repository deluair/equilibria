"""Public procurement efficiency and corruption risk analysis.

Key frameworks:

1. Procurement efficiency scoring (OECD 2019):
   Composite of: single-source rate, competitive tendering share,
   bid submission rate, contract execution efficiency, procurement cycle time.
   Higher single-source and lower competition -> efficiency loss and rent extraction.

2. Competition indicators (Szucs 2017, Fazekas & Kocsis 2020):
   Single bidder rate: share of contracts with exactly one bidder.
   Win rate concentration: Herfindahl-Hirschman index of contractor shares.
   Repeat winner rate: fraction of contracts won by same supplier repeatedly.
   High concentration is both efficiency risk and corruption marker.

3. E-procurement savings (World Bank 2016):
   Price savings from e-procurement systems typically 6-20% below manual.
   Savings = (manual_unit_price - e_proc_unit_price) / manual_unit_price
   E-procurement adoption rate = e_proc_value / total_procurement_value.

4. Corruption risk in bidding (Fazekas, Toth & King 2016):
   Corruption Risk Index (CRI): PCA-composite of red flags:
   - Single bidder, non-open procedure, short submission period,
     winner registered near decision, close bid-submission gap.
   CRI in [0,1]; higher values indicate elevated corruption risk.

References:
    OECD (2019). Going Digital in Public Procurement. OECD Digital Government.
    Szucs, F. (2017). Discretion and Corruption in Public Procurement. WP.
    Fazekas, M. & Kocsis, G. (2020). Uncovering High-Level Corruption. BJPS.
    Fazekas, M., Toth, I. & King, L. (2016). An Objective Corruption Risk Index.
        European Journal on Criminal Policy and Research, 22(3).
    World Bank (2016). How to Measure the Benefits of E-Procurement.

Sources: OECD MAPS, World Bank STEP, Open Contracting Data Standard (OCDS).
"""

from __future__ import annotations

import json

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class PublicProcurement(LayerBase):
    layer_id = "l10"
    name = "Public Procurement"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")
        results: dict = {"country": country}

        # --- Competition indicators ---
        competition_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('PROCUREMENT_COMPETITION', 'SINGLE_BIDDER_RATE')
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        competition_result = None
        if competition_rows:
            latest = competition_rows[0]
            single_bidder_rate = float(latest["value"])
            meta = json.loads(latest["metadata"]) if latest.get("metadata") else {}
            avg_bidders = meta.get("avg_bidders_per_lot", None)
            open_proc_share = meta.get("open_procedure_share", None)
            hhi_contractors = meta.get("contractor_hhi", None)

            # Bid submission rate trend
            if len(competition_rows) >= 3:
                sbr_vals = [float(r["value"]) for r in competition_rows[:10]]
                trend_slope = float(np.polyfit(np.arange(len(sbr_vals)), sbr_vals, 1)[0])
            else:
                trend_slope = None

            competition_result = {
                "single_bidder_rate": round(single_bidder_rate, 4),
                "avg_bidders_per_lot": round(float(avg_bidders), 2) if avg_bidders is not None else None,
                "open_procedure_share": round(float(open_proc_share), 4) if open_proc_share is not None else None,
                "contractor_hhi": round(float(hhi_contractors), 4) if hhi_contractors is not None else None,
                "single_bidder_trend": round(trend_slope, 4) if trend_slope is not None else None,
                "concentration_risk": (
                    "high" if single_bidder_rate > 0.30
                    else "moderate" if single_bidder_rate > 0.15
                    else "low"
                ),
            }
        results["competition"] = competition_result or {"error": "no competition data"}

        # --- E-procurement adoption and savings ---
        eproc_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('EPROC_ADOPTION_RATE', 'EPROC_VALUE_SHARE')
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )

        eproc_result = None
        if eproc_rows:
            meta = json.loads(eproc_rows[0]["metadata"]) if eproc_rows[0].get("metadata") else {}
            adoption_rate = float(eproc_rows[0]["value"])
            manual_price = meta.get("manual_avg_unit_price", None)
            eproc_price = meta.get("eproc_avg_unit_price", None)

            price_saving = None
            if manual_price and eproc_price and manual_price > 0:
                price_saving = (float(manual_price) - float(eproc_price)) / float(manual_price)

            eproc_result = {
                "adoption_rate": round(adoption_rate, 4),
                "price_saving_pct": round(price_saving * 100, 2) if price_saving is not None else None,
                "full_adoption": adoption_rate > 0.80,
                "maturity": (
                    "mature" if adoption_rate > 0.80
                    else "developing" if adoption_rate > 0.40
                    else "nascent"
                ),
            }
        results["e_procurement"] = eproc_result or {"error": "no e-procurement data"}

        # --- Procurement efficiency score ---
        efficiency_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('PROCUREMENT_EFFICIENCY_INDEX', 'PROC_CYCLE_TIME')
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )

        efficiency_result = None
        if efficiency_rows:
            meta = json.loads(efficiency_rows[0]["metadata"]) if efficiency_rows[0].get("metadata") else {}
            composite = float(efficiency_rows[0]["value"])
            cycle_days = meta.get("avg_cycle_days", None)
            contract_execution_rate = meta.get("contract_execution_rate", None)
            cancellation_rate = meta.get("cancellation_rate", None)

            efficiency_result = {
                "efficiency_index": round(composite, 2),
                "avg_cycle_days": round(float(cycle_days), 0) if cycle_days is not None else None,
                "contract_execution_rate": round(float(contract_execution_rate), 4) if contract_execution_rate is not None else None,
                "cancellation_rate": round(float(cancellation_rate), 4) if cancellation_rate is not None else None,
                "rating": (
                    "excellent" if composite > 75
                    else "good" if composite > 55
                    else "poor" if composite > 35
                    else "critical"
                ),
            }
        results["efficiency"] = efficiency_result or {"error": "no procurement efficiency data"}

        # --- Corruption risk index ---
        cri_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('CORRUPTION_RISK_INDEX', 'PROCUREMENT_CRI')
            ORDER BY dp.date
            """,
            (country,),
        )

        cri_result = None
        if cri_rows:
            latest_cri = cri_rows[-1]
            cri_val = float(latest_cri["value"])
            meta = json.loads(latest_cri["metadata"]) if latest_cri.get("metadata") else {}

            # Red flag counts
            red_flags = {k: v for k, v in meta.items() if k.startswith("rf_")}

            cri_series = [
                {"date": r["date"], "cri": round(float(r["value"]), 4)}
                for r in cri_rows[-10:]
            ]

            if len(cri_rows) >= 3:
                cri_vals_arr = np.array([float(r["value"]) for r in cri_rows[-10:]])
                t_arr = np.arange(len(cri_vals_arr))
                slope_cri, _, _, p_cri, _ = sp_stats.linregress(t_arr, cri_vals_arr)
            else:
                slope_cri, p_cri = None, None

            cri_result = {
                "corruption_risk_index": round(cri_val, 4),
                "risk_level": (
                    "critical" if cri_val > 0.7
                    else "high" if cri_val > 0.5
                    else "elevated" if cri_val > 0.3
                    else "low"
                ),
                "red_flags": red_flags,
                "trend": cri_series,
                "trend_slope": round(float(slope_cri), 5) if slope_cri is not None else None,
                "improving": slope_cri < 0 if slope_cri is not None else None,
            }
        results["corruption_risk"] = cri_result or {"error": "no corruption risk data"}

        # --- Score ---
        score = 25.0

        # Single bidder rate penalty
        if competition_result and not competition_result.get("error"):
            sbr = competition_result["single_bidder_rate"]
            score += min(25.0, sbr * 70.0)

        # Corruption risk penalty
        if cri_result and not cri_result.get("error"):
            cri_v = cri_result["corruption_risk_index"]
            score += cri_v * 30.0

        # Low e-procurement adoption
        if eproc_result and not eproc_result.get("error"):
            if eproc_result["maturity"] == "nascent":
                score += 10.0
            elif eproc_result["maturity"] == "developing":
                score += 5.0

        # Low efficiency
        if efficiency_result and not efficiency_result.get("error"):
            if efficiency_result["rating"] == "critical":
                score += 10.0
            elif efficiency_result["rating"] == "poor":
                score += 5.0

        score = float(np.clip(score, 0.0, 100.0))
        return {"score": round(score, 1), "results": results}
