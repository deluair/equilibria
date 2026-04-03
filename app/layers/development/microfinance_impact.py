"""Microfinance impact assessment: RCT evidence, repayment, and mission drift.

Four analytical components:

1. RCT-based impact estimates (Banerjee-Duflo meta-analysis): Synthesizes
   results from Banerjee et al. (2015) six-country RCT cluster. ATE on
   consumption: small positive (~3-7%) but not transformative. ATE on
   business investment: positive and significant for existing businesses.
   No detectable effect on income or women's empowerment on average.
   Uses inverse-variance weighting across available local estimates.

2. Repayment rates: portfolio at risk (PAR30, PAR90) from MFI Transparency
   / MIX Market data. PAR30 > 10% = stress signal. High repayment can
   reflect over-indebtedness (forced repayment priority) not genuine success.
   Schicks (2013) repayment burden framework used for interpretation.

3. Group lending dynamics: village bank / solidarity group models
   exhibit peer monitoring and joint liability. Ghatak (1999) assortative
   matching: homogeneous risk groups select together, improving screening.
   Variables: group size, dropout rate, group loan share of portfolio.

4. Mission drift detection: the commercial transformation hypothesis
   (Cull, Demirguc-Kunt & Morduch 2007). MFIs charging higher interest
   rates with larger average loan sizes and wealthier borrowers have drifted
   from the poverty mission. Measured via average loan balance / GNI per
   capita ratio (higher = drift) and interest rate spread vs. deposit rate.

References:
    Banerjee, A., Karlan, D. & Zinman, J. (2015). Six randomized evaluations
        of microcredit. American Economic Journal: Applied Economics 7(1).
    Ghatak, M. (1999). Group lending, local information, and peer selection.
        Journal of Development Economics 60(1): 27-50.
    Cull, R., Demirguc-Kunt, A. & Morduch, J. (2007). Financial performance
        and outreach: A global analysis of leading microbanks.
        Economic Journal 117(517): F107-F133.
    Schicks, J. (2013). The sacrifices of micro-borrowers in sub-Saharan Africa.
        Journal of Development Studies 49(9): 1199-1214.

Score: high PAR30 + evidence of mission drift + weak RCT impacts -> STRESS.
Low defaults, deep outreach, positive impacts -> STABLE.
"""

from __future__ import annotations

import json

import numpy as np

from app.layers.base import LayerBase


class MicrofinanceImpact(LayerBase):
    layer_id = "l4"
    name = "Microfinance Impact"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        country_iso3 = kwargs.get("country_iso3")

        # MFI portfolio data
        mfi_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value, ds.metadata, ds.series_id
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.source IN ('mix_market', 'mfi_transparency', 'microfinance')
              AND dp.value IS NOT NULL
              AND (:country IS NULL OR ds.country_iso3 = :country)
            ORDER BY ds.country_iso3, dp.date DESC
            """,
            {"country": country_iso3},
        )

        # RCT impact estimates stored as evidence layer
        rct_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.source = 'microfinance_rct'
              AND dp.value IS NOT NULL
              AND (:country IS NULL OR ds.country_iso3 = :country)
            ORDER BY dp.date DESC
            """,
            {"country": country_iso3},
        )

        # GNI per capita for mission drift normalization
        gni_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GNP.PCAP.CD'
              AND dp.value > 0
              AND (:country IS NULL OR ds.country_iso3 = :country)
            AND dp.date = (
                SELECT MAX(dp2.date) FROM data_points dp2
                WHERE dp2.series_id = ds.id AND dp2.value > 0
            )
            """,
            {"country": country_iso3},
        )

        gni_map = {r["country_iso3"]: float(r["value"]) for r in gni_rows}

        # Parse MFI data
        par30_vals: list[float] = []
        par90_vals: list[float] = []
        avg_loan_sizes: list[float] = []
        interest_rates: list[float] = []
        group_loan_shares: list[float] = []
        active_borrowers: list[float] = []

        seen_indicators: set[str] = set()
        for r in mfi_rows:
            meta = json.loads(r["metadata"]) if r.get("metadata") else {}
            indicator = meta.get("indicator", r.get("series_id", ""))
            iso = r["country_iso3"]
            val = float(r["value"])

            key = f"{iso}:{indicator}"
            if key in seen_indicators:
                continue
            seen_indicators.add(key)

            if "par30" in indicator or "portfolio_at_risk_30" in indicator:
                par30_vals.append(val)
            elif "par90" in indicator or "portfolio_at_risk_90" in indicator:
                par90_vals.append(val)
            elif "average_loan" in indicator or "avg_loan_balance" in indicator:
                avg_loan_sizes.append(val)
            elif "interest_rate" in indicator or "yield_on_portfolio" in indicator:
                interest_rates.append(val)
            elif "group_loan_share" in indicator or "solidarity_group" in indicator:
                group_loan_shares.append(val)
            elif "active_borrowers" in indicator:
                active_borrowers.append(val)

        # RCT impact meta-analysis
        rct_ates: list[float] = []
        rct_ses: list[float] = []
        for r in rct_rows:
            meta = json.loads(r["metadata"]) if r.get("metadata") else {}
            ate = r["value"]
            se = meta.get("standard_error")
            if ate is not None:
                rct_ates.append(float(ate))
                if se is not None:
                    rct_ses.append(float(se))

        # Inverse-variance weighted ATE
        rct_meta = None
        if rct_ates:
            if rct_ses and len(rct_ses) == len(rct_ates):
                ses = np.array(rct_ses)
                weights = 1.0 / (ses ** 2)
                iv_ate = float(np.sum(weights * np.array(rct_ates)) / np.sum(weights))
                iv_se = float(1.0 / np.sqrt(np.sum(weights)))
                rct_meta = {
                    "iv_weighted_ate": round(iv_ate, 4),
                    "iv_se": round(iv_se, 4),
                    "n_studies": len(rct_ates),
                    "interpretation": (
                        "positive consumption effect" if iv_ate > 0.05
                        else "small positive effect" if iv_ate > 0
                        else "null or negative average treatment effect"
                    ),
                }
            else:
                rct_meta = {
                    "simple_mean_ate": round(float(np.mean(rct_ates)), 4),
                    "n_studies": len(rct_ates),
                    "note": "no SE data for IV weighting",
                }

        # Portfolio quality
        par30 = float(np.mean(par30_vals)) if par30_vals else None
        par90 = float(np.mean(par90_vals)) if par90_vals else None

        # Mission drift: average loan balance / GNI per capita
        mission_drift_ratio = None
        target_gni = gni_map.get(country_iso3) if country_iso3 else None
        if avg_loan_sizes and target_gni and target_gni > 0:
            avg_loan = float(np.mean(avg_loan_sizes))
            mission_drift_ratio = avg_loan / target_gni
            # Ratio < 0.2 suggests outreach to the poor; > 0.5 suggests drift

        # Group lending share
        group_share = float(np.mean(group_loan_shares)) if group_loan_shares else None

        # Score construction
        score = 35.0  # moderate baseline

        # PAR30 contribution (benchmark: PAR30 < 5% = healthy)
        if par30 is not None:
            if par30 > 20:
                score += 30.0
            elif par30 > 10:
                score += 20.0
            elif par30 > 5:
                score += 8.0
            else:
                score -= 5.0

        # Mission drift (ratio > 0.5 = drift)
        if mission_drift_ratio is not None:
            if mission_drift_ratio > 0.5:
                score += 15.0
            elif mission_drift_ratio > 0.25:
                score += 5.0
            elif mission_drift_ratio < 0.15:
                score -= 5.0  # genuine poverty outreach

        # RCT impacts (positive ATE reduces score = better stability)
        if rct_meta:
            ate_val = rct_meta.get("iv_weighted_ate") or rct_meta.get("simple_mean_ate", 0)
            if ate_val is not None:
                if ate_val > 0.05:
                    score -= 8.0
                elif ate_val < 0:
                    score += 8.0

        score = max(0.0, min(100.0, score))

        results: dict = {
            "country_iso3": country_iso3,
            "rct_evidence": rct_meta,
            "portfolio_quality": {
                "par30_pct": round(par30, 2) if par30 is not None else None,
                "par90_pct": round(par90, 2) if par90 is not None else None,
                "assessment": (
                    "high default risk" if par30 is not None and par30 > 15
                    else "moderate risk" if par30 is not None and par30 > 7
                    else "healthy portfolio" if par30 is not None
                    else "data unavailable"
                ),
            },
            "mission_drift": {
                "avg_loan_gni_ratio": round(mission_drift_ratio, 3) if mission_drift_ratio is not None else None,
                "drift_detected": mission_drift_ratio is not None and mission_drift_ratio > 0.5,
                "interpretation": (
                    "significant mission drift toward upmarket clients"
                    if mission_drift_ratio is not None and mission_drift_ratio > 0.5
                    else "some upward drift" if mission_drift_ratio is not None and mission_drift_ratio > 0.25
                    else "poverty-focused outreach" if mission_drift_ratio is not None
                    else "data unavailable"
                ),
            },
            "group_lending": {
                "group_loan_share_pct": round(group_share * 100.0, 2) if group_share is not None else None,
            },
        }

        return {"score": round(score, 2), "results": results}
