"""Non-tariff measures (NTM) analysis: AVEs, coverage, and regulatory distance.

Methodology:
    Quantify the trade effects of non-tariff measures following UNCTAD
    classification and estimation methods:

    1. Ad valorem equivalents (AVEs):
       Convert NTMs into tariff-equivalent price effects using the
       quantity-based approach (Kee et al., 2009). For each NTM type:
       AVE_k = (P_ntm - P_free) / P_free
       estimated via gravity residuals or price-gap method.

    2. SPS/TBT trade effects:
       Estimate the trade-reducing (cost-raising) and trade-enhancing
       (quality-signaling) effects of SPS and TBT measures following
       Disdier et al. (2008). Separate restrictive from informational NTMs.

    3. NTM coverage and frequency ratios:
       Coverage ratio: share of trade (by value) subject to NTMs.
       Frequency ratio: share of tariff lines with at least one NTM.
       By NTM chapter: SPS (A), TBT (B), pre-shipment (C), contingent (D),
       non-automatic licensing (E), price control (F), finance (G),
       anti-competitive (H), export (P).

    4. Regulatory distance between trade partners:
       Measure how different NTM regimes are across partner pairs using
       Jaccard distance on NTM profiles. Higher distance = higher
       compliance costs for exporters adapting to different standards.

    Score (0-100): Higher score indicates more restrictive NTM environment
    (high AVEs, broad coverage, large regulatory distance from partners).

References:
    Kee, H.L. et al. (2009). "Estimating Trade Restrictiveness Indices."
        Economic Journal, 119(534), 172-199.
    Disdier, A.C. et al. (2008). "The Impact of Regulations on Agricultural
        Trade." American Journal of Agricultural Economics, 90(2), 336-350.
    UNCTAD (2019). "International Classification of Non-Tariff Measures."
"""

from __future__ import annotations

import numpy as np
from scipy.spatial.distance import jaccard

from app.layers.base import LayerBase

# UNCTAD NTM chapters
NTM_CHAPTERS = {
    "A": "SPS (Sanitary and Phytosanitary)",
    "B": "TBT (Technical Barriers to Trade)",
    "C": "Pre-shipment Inspection",
    "D": "Contingent Trade-Protective",
    "E": "Non-automatic Licensing",
    "F": "Price Control Measures",
    "G": "Finance Measures",
    "H": "Anti-competitive Measures",
    "P": "Export-related Measures",
}


class NTMAnalysis(LayerBase):
    layer_id = "l1"
    name = "NTM Analysis"

    async def compute(self, db, **kwargs) -> dict:
        """Compute NTM restrictiveness indicators.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default BGD)
            year : int - reference year
            partner_iso3 : str - bilateral partner (optional)
        """
        country = kwargs.get("country_iso3", "BGD")
        year = kwargs.get("year", 2022)
        partner = kwargs.get("partner_iso3")

        # Fetch NTM data by product and chapter
        ntm_rows = await db.fetch_all(
            """
            SELECT product_code, ntm_chapter, ntm_code, affected_partner,
                   ave_estimate, trade_value, is_sps, is_tbt
            FROM ntm_measures
            WHERE imposing_country = ?
              AND (year = ? OR year IS NULL)
            ORDER BY ntm_chapter, product_code
            """,
            (country, year),
        )

        if not ntm_rows:
            return {
                "score": 50.0,
                "note": "No NTM data available",
                "country": country,
                "year": year,
            }

        # Coverage and frequency ratios
        products_with_ntm = set()
        all_products = set()
        trade_with_ntm = 0.0
        total_trade = 0.0
        chapter_counts = {}
        aves = []
        sps_effects = []
        tbt_effects = []

        for r in ntm_rows:
            prod = r["product_code"]
            all_products.add(prod)
            tv = float(r["trade_value"] or 0)
            total_trade += tv

            if r["ntm_code"]:
                products_with_ntm.add(prod)
                trade_with_ntm += tv

                chapter = r["ntm_chapter"] or "unknown"
                chapter_counts[chapter] = chapter_counts.get(chapter, 0) + 1

                if r["ave_estimate"] is not None:
                    ave = float(r["ave_estimate"])
                    aves.append(ave)
                    if r["is_sps"]:
                        sps_effects.append(ave)
                    if r["is_tbt"]:
                        tbt_effects.append(ave)

        frequency_ratio = (
            len(products_with_ntm) / len(all_products) if all_products else 0.0
        )
        coverage_ratio = trade_with_ntm / total_trade if total_trade > 0 else 0.0

        # AVE statistics
        avg_ave = float(np.mean(aves)) if aves else None
        median_ave = float(np.median(aves)) if aves else None
        p90_ave = float(np.percentile(aves, 90)) if len(aves) >= 10 else None

        # SPS and TBT effects
        sps_avg = float(np.mean(sps_effects)) if sps_effects else None
        tbt_avg = float(np.mean(tbt_effects)) if tbt_effects else None

        # NTM intensity by chapter
        total_ntms = sum(chapter_counts.values()) if chapter_counts else 1
        chapter_shares = {
            ch: {
                "count": cnt,
                "share": round(cnt / total_ntms, 4),
                "label": NTM_CHAPTERS.get(ch, ch),
            }
            for ch, cnt in sorted(chapter_counts.items(), key=lambda x: x[1], reverse=True)
        }

        # Most affected products (highest AVE)
        product_aves = {}
        for r in ntm_rows:
            if r["ave_estimate"] is not None:
                prod = r["product_code"]
                if prod not in product_aves:
                    product_aves[prod] = []
                product_aves[prod].append(float(r["ave_estimate"]))

        top_restricted = sorted(
            [
                {"product": p, "avg_ave": round(float(np.mean(v)), 4), "n_ntms": len(v)}
                for p, v in product_aves.items()
            ],
            key=lambda x: x["avg_ave"],
            reverse=True,
        )[:15]

        # Regulatory distance to partners
        reg_distance = None
        if partner:
            partner_ntms = await db.fetch_all(
                """
                SELECT product_code, ntm_chapter, ntm_code
                FROM ntm_measures
                WHERE imposing_country = ?
                  AND (year = ? OR year IS NULL)
                """,
                (partner, year),
            )

            if partner_ntms:
                # Build binary NTM profiles
                all_prod_ntm = set()
                country_profile = set()
                partner_profile = set()

                for r in ntm_rows:
                    key = (r["product_code"], r["ntm_chapter"])
                    all_prod_ntm.add(key)
                    if r["ntm_code"]:
                        country_profile.add(key)

                for r in partner_ntms:
                    key = (r["product_code"], r["ntm_chapter"])
                    all_prod_ntm.add(key)
                    if r["ntm_code"]:
                        partner_profile.add(key)

                if all_prod_ntm:
                    c_vec = np.array([1 if k in country_profile else 0 for k in all_prod_ntm])
                    p_vec = np.array([1 if k in partner_profile else 0 for k in all_prod_ntm])
                    # Avoid zero vectors
                    if c_vec.sum() > 0 and p_vec.sum() > 0:
                        jac_dist = float(jaccard(c_vec, p_vec))
                        reg_distance = {
                            "partner": partner,
                            "jaccard_distance": round(jac_dist, 4),
                            "n_product_ntm_pairs": len(all_prod_ntm),
                            "country_ntm_count": int(c_vec.sum()),
                            "partner_ntm_count": int(p_vec.sum()),
                        }
        else:
            # Compute average distance to top trade partners
            top_partners = await db.fetch_all(
                """
                SELECT DISTINCT ds.metadata AS partner_iso3
                FROM data_series ds
                WHERE ds.source IN ('baci', 'comtrade')
                  AND ds.country_iso3 = ?
                LIMIT 10
                """,
                (country,),
            )

            if top_partners:
                distances = []
                country_keys = set()
                for r in ntm_rows:
                    if r["ntm_code"]:
                        country_keys.add((r["product_code"], r["ntm_chapter"]))

                for tp in top_partners:
                    p_iso = tp["partner_iso3"]
                    p_rows = await db.fetch_all(
                        """
                        SELECT product_code, ntm_chapter, ntm_code
                        FROM ntm_measures
                        WHERE imposing_country = ?
                          AND (year = ? OR year IS NULL)
                        """,
                        (p_iso, year),
                    )
                    if p_rows:
                        p_keys = set()
                        for pr in p_rows:
                            if pr["ntm_code"]:
                                p_keys.add((pr["product_code"], pr["ntm_chapter"]))
                        all_keys = country_keys | p_keys
                        if all_keys and country_keys and p_keys:
                            c_v = np.array([1 if k in country_keys else 0 for k in all_keys])
                            p_v = np.array([1 if k in p_keys else 0 for k in all_keys])
                            distances.append(float(jaccard(c_v, p_v)))

                if distances:
                    reg_distance = {
                        "avg_jaccard_distance": round(float(np.mean(distances)), 4),
                        "n_partners_compared": len(distances),
                    }

        # Score computation
        # High coverage ratio (max 25 points)
        coverage_penalty = coverage_ratio * 25.0

        # High AVEs (max 30 points)
        ave_val = avg_ave if avg_ave is not None else 10.0
        ave_penalty = min(ave_val / 50.0, 1.0) * 30.0  # 50% AVE -> max

        # High frequency ratio (max 20 points)
        freq_penalty = frequency_ratio * 20.0

        # Regulatory distance (max 25 points)
        dist_val = 0.5  # default
        if reg_distance:
            dist_val = reg_distance.get(
                "jaccard_distance", reg_distance.get("avg_jaccard_distance", 0.5)
            )
        distance_penalty = dist_val * 25.0

        score = float(np.clip(
            coverage_penalty + ave_penalty + freq_penalty + distance_penalty,
            0, 100,
        ))

        result = {
            "score": round(score, 2),
            "country": country,
            "year": year,
            "frequency_ratio": round(frequency_ratio, 4),
            "coverage_ratio": round(coverage_ratio, 4),
            "n_products_total": len(all_products),
            "n_products_with_ntm": len(products_with_ntm),
            "total_ntm_count": total_ntms,
            "avg_ave_pct": round(avg_ave, 4) if avg_ave is not None else None,
            "median_ave_pct": round(median_ave, 4) if median_ave is not None else None,
            "p90_ave_pct": round(p90_ave, 4) if p90_ave is not None else None,
            "sps_avg_ave": round(sps_avg, 4) if sps_avg is not None else None,
            "tbt_avg_ave": round(tbt_avg, 4) if tbt_avg is not None else None,
            "ntm_by_chapter": chapter_shares,
            "top_restricted_products": top_restricted,
            "regulatory_distance": reg_distance,
        }

        return result
