"""Network industries: natural monopoly regulation, unbundling, and access pricing.

Methodology
-----------
1. **Natural Monopoly Test (Baumol-Bradford-Willig)**:
   An industry exhibits natural monopoly when the cost function is subadditive:
     C(q1 + q2) < C(q1) + C(q2)  for all q1, q2 > 0
   Operationalized via scale economies index:
     S = AC / MC  (S > 1 -> economies of scale -> natural monopoly tendency)
   Ray average cost: C(q) / q along the output ray.
   Declining ray average cost (DRAC) is sufficient for subadditivity.

2. **Unbundling Effectiveness**:
   Vertical separation (structural unbundling) vs. accounting separation
   (functional unbundling) vs. no separation (integrated monopoly).
   Laffont-Tirole (1993) access pricing framework.
   Effectiveness metric: downstream competition growth post-unbundling,
   measured as HHI change before/after, normalized by access quality index.

3. **Access Pricing: Efficient Component Pricing Rule (ECPR)**:
   Baumol (1983) / Willig (1979) ECPR:
     a = p_retail - IC_incumbent + IC_entrant
   where a = access price, p_retail = retail price,
   IC = incremental cost of retail service.
   Efficient if IC_entrant > IC_incumbent (entrant is less efficient).
   Ramsey access price (second-best): trades off efficiency and revenue adequacy.
     pi_i / pi_j = (1/epsilon_i - 1/epsilon_j)^{-1}  [inverse-elasticity rule]

4. **Universal Service Obligation (USO) Cost**:
   Net cost of USO = profit from universal service - profit from cherry-picking:
     USO_cost = pi_universal - pi_cherry_pick
   Financing mechanisms: USO fund contributions, retail price averaging,
   direct subsidies. Burden measure as share of sector revenue.

References:
    Baumol, W.J., Panzar, J.C. & Willig, R.D. (1982). Contestable Markets and
        the Theory of Industry Structure. Harcourt Brace.
    Laffont, J.-J. & Tirole, J. (1993). A Theory of Incentives in Procurement
        and Regulation. MIT Press.
    Armstrong, M. (2002). The Theory of Access Pricing and Interconnection. In
        Cave, Majumdar & Vogelsang (Eds.), Handbook of Telecommunications Economics.
    Baumol, W.J. (1983). Some Subtle Issues in Railroad Regulation. IJTT 10(4).

Score: high USO cost + poor unbundling + access pricing inefficiency -> STRESS.
"""

from __future__ import annotations

import json

import numpy as np

from app.layers.base import LayerBase


class NetworkIndustries(LayerBase):
    layer_id = "l14"
    name = "Network Industries"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")
        sector = kwargs.get("sector")
        year = kwargs.get("year")

        clauses = ["ds.country_iso3 = ?", "ds.source = ?"]
        params: list = [country, "network_industries"]
        if sector:
            clauses.append("ds.description LIKE ?")
            params.append(f"%{sector}%")
        if year:
            clauses.append("dp.date = ?")
            params.append(str(year))

        where = " AND ".join(clauses)
        rows = await db.fetch_all(
            f"""
            SELECT dp.value, dp.date, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE {where}
            ORDER BY dp.date DESC
            """,
            tuple(params),
        )

        if not rows or len(rows) < 2:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient network industry data"}

        firms = []
        for row in rows:
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            firms.append({
                "total_cost": float(meta["total_cost"]) if meta.get("total_cost") is not None else None,
                "output": float(meta["output"]) if meta.get("output") is not None else None,
                "marginal_cost": float(meta["marginal_cost"]) if meta.get("marginal_cost") is not None else None,
                "access_price": float(meta["access_price"]) if meta.get("access_price") is not None else None,
                "retail_price": float(meta["retail_price"]) if meta.get("retail_price") is not None else None,
                "incremental_cost": float(meta["incremental_cost"]) if meta.get("incremental_cost") is not None else None,
                "market_share": float(meta["market_share"]) if meta.get("market_share") is not None else None,
                "uso_revenue_share": float(meta["uso_revenue_share"]) if meta.get("uso_revenue_share") is not None else None,
                "unbundling_type": meta.get("unbundling_type"),
                "access_quality": float(meta["access_quality"]) if meta.get("access_quality") is not None else None,
                "demand_elasticity": float(meta["demand_elasticity"]) if meta.get("demand_elasticity") is not None else None,
            })

        natural_monopoly = self._natural_monopoly_test(firms)
        unbundling = self._unbundling_effectiveness(firms)
        ecpr = self._ecpr_assessment(firms)
        uso = self._uso_cost(firms)

        # Score based on regulation gap: high scale economies + poor regulation
        score_components = []

        if natural_monopoly and natural_monopoly.get("scale_economy_index") is not None:
            s_index = float(natural_monopoly["scale_economy_index"])
            # S > 1.5 -> strong natural monopoly tendency
            nm_score = min((s_index - 1.0) / 1.0 * 40.0, 40.0) if s_index > 1.0 else 0.0
            score_components.append(nm_score)

        if ecpr and ecpr.get("markup_over_ecpr") is not None:
            ecpr_markup = float(ecpr["markup_over_ecpr"])
            ecpr_score = min(abs(ecpr_markup) / 0.5 * 30.0, 30.0)
            score_components.append(ecpr_score)

        if uso and uso.get("uso_burden_index") is not None:
            uso_score = min(float(uso["uso_burden_index"]) * 30.0, 30.0)
            score_components.append(uso_score)

        score = float(np.mean(score_components)) if score_components else 50.0
        score = max(0.0, min(100.0, score))

        return {
            "score": round(score, 2),
            "country": country,
            "natural_monopoly": natural_monopoly,
            "unbundling": unbundling,
            "ecpr": ecpr,
            "uso": uso,
        }

    @staticmethod
    def _natural_monopoly_test(firms: list[dict]) -> dict | None:
        """Baumol-Willig scale economies and subadditivity test."""
        valid = [f for f in firms if f["total_cost"] is not None and f["output"] is not None and f["output"] > 0]
        if not valid:
            return None

        costs = np.array([f["total_cost"] for f in valid], dtype=float)
        outputs = np.array([f["output"] for f in valid], dtype=float)

        avg_costs = costs / outputs
        # Scale economy index: S = AC / MC
        mcs = [f["marginal_cost"] for f in valid if f["marginal_cost"] is not None]
        if mcs and len(mcs) == len(valid):
            mc_arr = np.array(mcs, dtype=float)
            scale_index = avg_costs / np.maximum(mc_arr, 1e-10)
            mean_scale = float(np.mean(scale_index))
        else:
            # Estimate MC via log-log regression: ln(C) = a + b*ln(q)
            if len(outputs) >= 4:
                X = np.column_stack([np.ones(len(outputs)), np.log(np.maximum(outputs, 1e-10))])
                b = np.linalg.lstsq(X, np.log(np.maximum(costs, 1e-10)), rcond=None)[0]
                # b[1] is cost elasticity; S = 1/b[1]
                mean_scale = float(1.0 / b[1]) if abs(b[1]) > 1e-10 else float("inf")
            else:
                mean_scale = None

        # DRAC test: is average cost declining?
        if len(avg_costs) >= 3:
            slope = float(np.polyfit(outputs, avg_costs, 1)[0])
            drac = slope < 0
        else:
            drac = None

        return {
            "scale_economy_index": round(mean_scale, 4) if mean_scale is not None and not np.isinf(mean_scale) else None,
            "natural_monopoly_indicator": mean_scale > 1.0 if mean_scale is not None else None,
            "drac": drac,
            "n_firms": len(valid),
            "mean_avg_cost": round(float(np.mean(avg_costs)), 4),
        }

    @staticmethod
    def _unbundling_effectiveness(firms: list[dict]) -> dict | None:
        """HHI change and access quality as unbundling effectiveness metrics."""
        shares = [f["market_share"] for f in firms if f["market_share"] is not None]
        qualities = [f["access_quality"] for f in firms if f["access_quality"] is not None]
        unbundling_types = [f["unbundling_type"] for f in firms if f["unbundling_type"] is not None]

        if not shares:
            return None

        s_arr = np.array(shares, dtype=float)
        s_arr = s_arr / s_arr.sum() if s_arr.sum() > 0 else s_arr
        hhi = float(np.sum(s_arr ** 2))

        avg_quality = float(np.mean(qualities)) if qualities else None

        # Dominant unbundling type
        if unbundling_types:
            from collections import Counter
            dominant_type = Counter(unbundling_types).most_common(1)[0][0]
        else:
            dominant_type = None

        # Effectiveness score: lower HHI + higher quality = more effective
        effectiveness = None
        if avg_quality is not None:
            effectiveness = round((1.0 - hhi) * avg_quality, 4)

        return {
            "downstream_hhi": round(hhi, 4),
            "avg_access_quality": round(avg_quality, 4) if avg_quality is not None else None,
            "dominant_unbundling_type": dominant_type,
            "effectiveness_score": effectiveness,
        }

    @staticmethod
    def _ecpr_assessment(firms: list[dict]) -> dict | None:
        """Efficient Component Pricing Rule assessment."""
        valid = [
            f for f in firms
            if f["access_price"] is not None
            and f["retail_price"] is not None
            and f["incremental_cost"] is not None
        ]
        if not valid:
            return None

        ecpr_prices = []
        for f in valid:
            # ECPR: a* = p_retail - IC_incumbent + IC_entrant
            # Approximate: IC_entrant ~ IC_incumbent (competitive), ECPR: a* = p_retail - IC
            ecpr_star = f["retail_price"] - f["incremental_cost"]
            ecpr_prices.append(ecpr_star)

        actual_access = np.array([f["access_price"] for f in valid], dtype=float)
        ecpr_star_arr = np.array(ecpr_prices, dtype=float)

        markup_over_ecpr = float(np.mean(actual_access - ecpr_star_arr))
        relative_markup = markup_over_ecpr / float(np.mean(ecpr_star_arr)) if float(np.mean(ecpr_star_arr)) > 0 else None

        # Ramsey efficiency: check if demand elasticities allow inverse-elasticity pricing
        elasticities = [f["demand_elasticity"] for f in valid if f["demand_elasticity"] is not None]
        ramsey_applicable = len(elasticities) >= 2

        return {
            "mean_actual_access_price": round(float(np.mean(actual_access)), 4),
            "mean_ecpr_price": round(float(np.mean(ecpr_star_arr)), 4),
            "markup_over_ecpr": round(markup_over_ecpr, 4),
            "relative_markup": round(relative_markup, 4) if relative_markup is not None else None,
            "access_pricing_efficient": abs(markup_over_ecpr) < 0.05 * float(np.mean(ecpr_star_arr)),
            "ramsey_applicable": ramsey_applicable,
            "n_observations": len(valid),
        }

    @staticmethod
    def _uso_cost(firms: list[dict]) -> dict | None:
        """Universal service obligation cost estimation."""
        uso_shares = [f["uso_revenue_share"] for f in firms if f["uso_revenue_share"] is not None]
        if not uso_shares:
            return None

        uso_arr = np.array(uso_shares, dtype=float)
        avg_uso = float(np.mean(uso_arr))
        max_uso = float(np.max(uso_arr))

        # Burden index: USO cost / sector revenue; > 0.1 is high burden
        uso_burden_index = avg_uso
        burden_level = (
            "high" if uso_burden_index > 0.10
            else "moderate" if uso_burden_index > 0.05
            else "low"
        )

        return {
            "avg_uso_revenue_share": round(avg_uso, 4),
            "max_uso_revenue_share": round(max_uso, 4),
            "uso_burden_index": round(uso_burden_index, 4),
            "burden_level": burden_level,
            "n_firms": len(uso_shares),
        }
