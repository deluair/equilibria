"""Rules of origin restrictiveness and preference utilization analysis.

Methodology:
    Analyze the complexity and trade impact of rules of origin (ROO) in
    preferential trade agreements:

    1. ROO Restrictiveness Index:
       Following Estevadeordal (2000) and Cadot et al. (2006), score ROO
       on a 1-7 scale based on:
       - Change of tariff classification (CTC) requirements
       - Value content (VC) thresholds
       - Technical requirements (specific process rules)
       - Exception provisions
       Product-level scores aggregated using trade weights.

    2. Compliance costs:
       Administrative costs of proving origin (documentation, certification,
       verification). Estimated as ad valorem equivalent of compliance burden.
       Following Cadot & Ing (2016): typically 2-8% of trade value.

    3. Spaghetti bowl effect:
       Count overlapping FTAs with different ROO for same products.
       Measure ROO divergence across agreements using Hamming distance.
       Higher divergence = more complex compliance landscape.

    4. Utilization rate of trade preferences:
       Preferential imports / eligible imports. Low utilization implies ROO
       are too restrictive relative to preference margin.
       Following Keck & Lendle (2012).

    Score (0-100): Higher score indicates more restrictive/complex ROO
    environment (low utilization, high compliance costs, spaghetti bowl).

References:
    Estevadeordal, A. (2000). "Negotiating Preferential Market Access:
        The Case of NAFTA." Journal of World Trade, 34(1), 141-166.
    Cadot, O. et al. (2006). "The Origin of Goods: Rules of Origin in
        Regional Trade Agreements." Oxford University Press.
    Keck, A. & Lendle, A. (2012). "New Evidence on Preference Utilization."
        WTO Staff Working Paper ERSD-2012-12.
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class RulesOfOrigin(LayerBase):
    layer_id = "l1"
    name = "Rules of Origin"

    async def compute(self, db, **kwargs) -> dict:
        """Compute ROO restrictiveness and preference utilization.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default BGD)
            year : int - reference year
        """
        country = kwargs.get("country_iso3", "BGD")
        year = kwargs.get("year", 2022)

        # Fetch ROO data by agreement and product
        roo_rows = await db.fetch_all(
            """
            SELECT agreement, product_code, roo_type, restrictiveness_score,
                   value_content_threshold, ctc_level, compliance_cost_pct
            FROM rules_of_origin
            WHERE country_iso3 = ?
            ORDER BY agreement, product_code
            """,
            (country,),
        )

        if not roo_rows:
            return {
                "score": 50.0,
                "note": "No rules of origin data available",
                "country": country,
                "year": year,
            }

        # Aggregate restrictiveness by agreement
        agreements = {}
        all_scores = []
        all_compliance = []

        for r in roo_rows:
            agr = r["agreement"]
            if agr not in agreements:
                agreements[agr] = {
                    "products": [],
                    "scores": [],
                    "compliance_costs": [],
                    "roo_types": {},
                    "vc_thresholds": [],
                }

            score = float(r["restrictiveness_score"] or 0)
            agreements[agr]["products"].append(r["product_code"])
            agreements[agr]["scores"].append(score)
            all_scores.append(score)

            if r["compliance_cost_pct"] is not None:
                cost = float(r["compliance_cost_pct"])
                agreements[agr]["compliance_costs"].append(cost)
                all_compliance.append(cost)

            roo_type = r["roo_type"] or "unspecified"
            agreements[agr]["roo_types"][roo_type] = (
                agreements[agr]["roo_types"].get(roo_type, 0) + 1
            )

            if r["value_content_threshold"] is not None:
                agreements[agr]["vc_thresholds"].append(
                    float(r["value_content_threshold"])
                )

        # Summary by agreement
        agreement_summary = {}
        for agr, data in agreements.items():
            scores_arr = np.array(data["scores"])
            agreement_summary[agr] = {
                "n_products": len(data["products"]),
                "avg_restrictiveness": round(float(np.mean(scores_arr)), 4),
                "max_restrictiveness": round(float(np.max(scores_arr)), 4),
                "roo_type_distribution": data["roo_types"],
                "avg_compliance_cost_pct": round(
                    float(np.mean(data["compliance_costs"])), 4
                ) if data["compliance_costs"] else None,
                "avg_vc_threshold": round(
                    float(np.mean(data["vc_thresholds"])), 4
                ) if data["vc_thresholds"] else None,
            }

        # Overall restrictiveness
        overall_restrictiveness = float(np.mean(all_scores)) if all_scores else 0.0
        avg_compliance_cost = float(np.mean(all_compliance)) if all_compliance else 4.0

        # Spaghetti bowl effect: count overlapping agreements per product
        product_agreements = {}
        for agr, data in agreements.items():
            for prod in data["products"]:
                if prod not in product_agreements:
                    product_agreements[prod] = set()
                product_agreements[prod].add(agr)

        overlap_counts = [len(agrs) for agrs in product_agreements.values()]
        avg_overlap = float(np.mean(overlap_counts)) if overlap_counts else 1.0
        max_overlap = max(overlap_counts) if overlap_counts else 1
        n_products_multi_roo = sum(1 for c in overlap_counts if c > 1)

        # ROO divergence across agreements (simplified Hamming distance)
        # Compare restrictiveness scores for products covered by multiple FTAs
        divergence_scores = []
        for prod, agrs in product_agreements.items():
            if len(agrs) < 2:
                continue
            agr_list = list(agrs)
            prod_scores_by_agr = []
            for agr in agr_list:
                idx = agreements[agr]["products"].index(prod)
                prod_scores_by_agr.append(agreements[agr]["scores"][idx])
            if len(prod_scores_by_agr) >= 2:
                divergence_scores.append(float(np.std(prod_scores_by_agr)))

        avg_divergence = float(np.mean(divergence_scores)) if divergence_scores else 0.0

        # Preference utilization rate
        util_rows = await db.fetch_all(
            """
            SELECT ds.metadata AS agreement, dp.date, dp.value AS utilization_rate
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source = 'preference_utilization'
              AND ds.country_iso3 = ?
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        utilization_by_agr = {}
        for r in util_rows:
            agr = r["agreement"]
            if agr not in utilization_by_agr and r["utilization_rate"] is not None:
                utilization_by_agr[agr] = float(r["utilization_rate"])

        avg_utilization = (
            float(np.mean(list(utilization_by_agr.values())))
            if utilization_by_agr
            else None
        )

        # Utilization trend
        util_trend = None
        if len(util_rows) >= 3:
            years_arr = []
            vals_arr = []
            for r in util_rows:
                if r["utilization_rate"] is not None:
                    try:
                        years_arr.append(float(r["date"][:4]))
                        vals_arr.append(float(r["utilization_rate"]))
                    except (ValueError, TypeError):
                        pass
            if len(years_arr) >= 3:
                slope, _, r_val, p_val, _ = stats.linregress(years_arr, vals_arr)
                util_trend = {
                    "slope_pct_per_year": round(float(slope), 4),
                    "r_squared": round(float(r_val ** 2), 4),
                    "p_value": round(float(p_val), 4),
                    "direction": "improving" if slope > 0 else "declining",
                }

        # Preference margin analysis
        # ROO only matter if preference margin is significant
        margin_rows = await db.fetch_all(
            """
            SELECT ds.metadata AS agreement, dp.value AS margin_pct
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source = 'preference_margin'
              AND ds.country_iso3 = ?
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        margins = {}
        for r in margin_rows:
            agr = r["agreement"]
            if agr not in margins and r["margin_pct"] is not None:
                margins[agr] = float(r["margin_pct"])

        avg_margin = float(np.mean(list(margins.values()))) if margins else None

        # Score computation
        # High restrictiveness (max 30 points), scale is 1-7
        restrict_penalty = min(overall_restrictiveness / 7.0, 1.0) * 30.0

        # Low utilization (max 25 points)
        util_val = avg_utilization if avg_utilization is not None else 50.0
        util_penalty = (1.0 - min(util_val / 100.0, 1.0)) * 25.0

        # High compliance costs (max 20 points)
        compliance_penalty = min(avg_compliance_cost / 10.0, 1.0) * 20.0

        # Spaghetti bowl complexity (max 25 points)
        bowl_penalty = min((avg_overlap - 1.0) / 4.0, 1.0) * 15.0
        divergence_penalty = min(avg_divergence / 2.0, 1.0) * 10.0

        score = float(np.clip(
            restrict_penalty + util_penalty + compliance_penalty
            + bowl_penalty + divergence_penalty,
            0, 100,
        ))

        return {
            "score": round(score, 2),
            "country": country,
            "year": year,
            "n_agreements": len(agreements),
            "n_products_covered": len(product_agreements),
            "overall_restrictiveness": round(overall_restrictiveness, 4),
            "avg_compliance_cost_pct": round(avg_compliance_cost, 4),
            "agreement_summary": agreement_summary,
            "spaghetti_bowl": {
                "avg_agreements_per_product": round(avg_overlap, 2),
                "max_overlap": max_overlap,
                "n_products_multi_roo": n_products_multi_roo,
                "avg_roo_divergence": round(avg_divergence, 4),
            },
            "preference_utilization": {
                "by_agreement": {k: round(v, 2) for k, v in utilization_by_agr.items()}
                if utilization_by_agr else None,
                "average": round(avg_utilization, 2) if avg_utilization else None,
                "trend": util_trend,
            },
            "preference_margins": {k: round(v, 2) for k, v in margins.items()}
            if margins else None,
            "avg_preference_margin": round(avg_margin, 2) if avg_margin else None,
        }
