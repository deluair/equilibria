"""Platform economics: two-sided markets, network effects, and regulation.

Two-sided market pricing (Rochet & Tirole 2003, 2006): a platform serving
two sides (buyers/sellers, users/advertisers) sets prices to balance
participation on both sides. The key insight is that the price structure
(not just the price level) matters:

    p_B + p_S = c + markup  (price level)
    But optimal split depends on cross-side externalities.

Platform profit maximization:
    max (p_B - c_B)*n_B + (p_S - c_S)*n_S
    s.t. n_B = f(p_B, n_S)  and  n_S = g(p_S, n_B)

where cross-side externality: d(n_B)/d(n_S) = alpha > 0.

Network effects estimation:
    Direct: user value increasing in same-side users (Metcalfe's law)
    Indirect (cross-side): user value increasing in other-side users
    V_B(n_B, n_S) = v_B + alpha_B * ln(n_S) + beta_B * ln(n_B)

Winner-take-all dynamics occur when:
    - Strong network effects (alpha large)
    - Low multi-homing costs
    - High switching costs
    Measured by single-firm dominance ratio and market tipping speed.

Platform regulation effects (Evans & Schmalensee 2015):
    - Price caps on one side may harm the other side
    - Mandated interoperability reduces network effects but increases competition
    - Data portability affects switching costs

References:
    Rochet, J.-C. & Tirole, J. (2003). Platform Competition in Two-Sided
        Markets. JEEA 1(4): 990-1029.
    Rochet, J.-C. & Tirole, J. (2006). Two-Sided Markets: A Progress
        Report. RAND Journal of Economics 37(3): 645-667.
    Evans, D. & Schmalensee, R. (2015). The Antitrust Analysis of
        Multi-Sided Platform Businesses. In Blair & Sokol (Eds.), Oxford
        Handbook of International Antitrust Economics, Vol. 1.

Score: high concentration + strong network effects -> STRESS (tipping risk),
balanced platforms -> STABLE.
"""

import json

import numpy as np

from app.layers.base import LayerBase


class PlatformEconomics(LayerBase):
    layer_id = "l14"
    name = "Platform Economics"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")
        sector = kwargs.get("sector")
        year = kwargs.get("year")

        clauses = ["ds.country_iso3 = ?", "ds.source = ?"]
        params: list = [country, "platform_economics"]
        if sector:
            clauses.append("ds.description LIKE ?")
            params.append(f"%{sector}%")
        if year:
            clauses.append("dp.date = ?")
            params.append(str(year))

        where = " AND ".join(clauses)
        rows = await db.fetch_all(
            f"""
            SELECT dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE {where}
            ORDER BY dp.date DESC
            """,
            tuple(params),
        )

        if not rows or len(rows) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient platform data"}

        platforms = []
        for row in rows:
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            platforms.append({
                "users_side_a": float(meta["users_side_a"]) if meta.get("users_side_a") is not None else None,
                "users_side_b": float(meta["users_side_b"]) if meta.get("users_side_b") is not None else None,
                "price_side_a": float(meta["price_side_a"]) if meta.get("price_side_a") is not None else None,
                "price_side_b": float(meta["price_side_b"]) if meta.get("price_side_b") is not None else None,
                "cost_side_a": float(meta["cost_side_a"]) if meta.get("cost_side_a") is not None else None,
                "cost_side_b": float(meta["cost_side_b"]) if meta.get("cost_side_b") is not None else None,
                "revenue": float(row["value"]) if row["value"] is not None else None,
                "market_share": float(meta["market_share"]) if meta.get("market_share") is not None else None,
                "multi_homing_rate": float(meta["multi_homing_rate"]) if meta.get("multi_homing_rate") is not None else None,
                "switching_cost": float(meta["switching_cost"]) if meta.get("switching_cost") is not None else None,
            })

        # Two-sided pricing analysis
        pricing = self._two_sided_pricing(platforms)

        # Network effects estimation
        network_effects = self._estimate_network_effects(platforms)

        # Winner-take-all dynamics
        wta = self._winner_take_all(platforms)

        # Regulation effects assessment
        regulation = self._regulation_assessment(platforms)

        # Score: combine concentration and network effects
        concentration_score = 0.0
        shares = [p["market_share"] for p in platforms if p["market_share"] is not None]
        if shares:
            shares_arr = np.array(sorted(shares, reverse=True))
            shares_arr = shares_arr / shares_arr.sum() if shares_arr.sum() > 0 else shares_arr
            hhi = float(np.sum(shares_arr ** 2))
            top_share = float(shares_arr[0])
            concentration_score = min(hhi * 100.0, 50.0)
            if top_share > 0.6:
                concentration_score += 20.0

        network_score = 0.0
        if network_effects and network_effects.get("cross_side_elasticity") is not None:
            alpha = abs(network_effects["cross_side_elasticity"])
            network_score = min(alpha * 30.0, 30.0)

        score = max(0.0, min(100.0, concentration_score + network_score))

        return {
            "score": round(score, 2),
            "country": country,
            "n_platforms": len(platforms),
            "two_sided_pricing": pricing,
            "network_effects": network_effects,
            "winner_take_all": wta,
            "regulation_assessment": regulation,
        }

    @staticmethod
    def _two_sided_pricing(platforms: list[dict]) -> dict | None:
        """Analyze Rochet-Tirole two-sided pricing structure."""
        valid = [p for p in platforms
                 if p["price_side_a"] is not None and p["price_side_b"] is not None]
        if not valid:
            return None

        prices_a = np.array([p["price_side_a"] for p in valid])
        prices_b = np.array([p["price_side_b"] for p in valid])
        total_prices = prices_a + prices_b

        # Price structure: which side is subsidized?
        costs_a = np.array([p["cost_side_a"] or 0 for p in valid])
        costs_b = np.array([p["cost_side_b"] or 0 for p in valid])

        margins_a = prices_a - costs_a
        margins_b = prices_b - costs_b

        # Subsidy side: negative margin indicates subsidized side
        mean_margin_a = float(np.mean(margins_a))
        mean_margin_b = float(np.mean(margins_b))

        if mean_margin_a < mean_margin_b:
            subsidized_side = "side_a"
            subsidy_magnitude = float(-np.mean(margins_a)) if mean_margin_a < 0 else 0.0
        else:
            subsidized_side = "side_b"
            subsidy_magnitude = float(-np.mean(margins_b)) if mean_margin_b < 0 else 0.0

        # Price level vs price structure
        price_level = float(np.mean(total_prices))
        price_ratio = float(np.mean(prices_a) / np.mean(prices_b)) if np.mean(prices_b) > 0 else float("inf")

        return {
            "mean_price_side_a": round(float(np.mean(prices_a)), 2),
            "mean_price_side_b": round(float(np.mean(prices_b)), 2),
            "price_level": round(price_level, 2),
            "price_ratio_a_to_b": round(price_ratio, 4),
            "subsidized_side": subsidized_side,
            "subsidy_magnitude": round(subsidy_magnitude, 2),
            "margin_side_a": round(mean_margin_a, 2),
            "margin_side_b": round(mean_margin_b, 2),
        }

    @staticmethod
    def _estimate_network_effects(platforms: list[dict]) -> dict | None:
        """Estimate direct and cross-side network effects.

        V_B = v_B + alpha * ln(n_S) + beta * ln(n_B)
        Use revenue as proxy for value.
        """
        valid = [p for p in platforms
                 if p["users_side_a"] is not None and p["users_side_b"] is not None
                 and p["revenue"] is not None]
        if len(valid) < 5:
            return None

        revenue = np.array([p["revenue"] for p in valid])
        n_a = np.array([max(p["users_side_a"], 1.0) for p in valid])
        n_b = np.array([max(p["users_side_b"], 1.0) for p in valid])

        # Regress ln(revenue) on ln(n_a), ln(n_b)
        y = np.log(np.maximum(revenue, 1.0))
        X = np.column_stack([np.ones(len(valid)), np.log(n_a), np.log(n_b)])

        beta = np.linalg.lstsq(X, y, rcond=None)[0]
        y_hat = X @ beta
        ss_res = float(np.sum((y - y_hat) ** 2))
        ss_tot = float(np.sum((y - np.mean(y)) ** 2))
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

        # Cross-side elasticity: coefficient on other side's users
        cross_side_elasticity = float(beta[2])  # elasticity w.r.t. side B users
        same_side_elasticity = float(beta[1])  # elasticity w.r.t. side A users

        # Metcalfe's law test: does value scale with n^2?
        total_users = n_a + n_b
        metcalfe_corr = float(np.corrcoef(np.log(revenue + 1), np.log(total_users ** 2 + 1))[0, 1])

        return {
            "cross_side_elasticity": round(cross_side_elasticity, 4),
            "same_side_elasticity": round(same_side_elasticity, 4),
            "r_squared": round(r2, 4),
            "metcalfe_correlation": round(metcalfe_corr, 4),
            "strong_network_effects": abs(cross_side_elasticity) > 0.5,
        }

    @staticmethod
    def _winner_take_all(platforms: list[dict]) -> dict | None:
        """Assess winner-take-all dynamics."""
        shares = [p["market_share"] for p in platforms if p["market_share"] is not None]
        if len(shares) < 2:
            return None

        shares_arr = np.array(sorted(shares, reverse=True))
        shares_arr = shares_arr / shares_arr.sum() if shares_arr.sum() > 0 else shares_arr

        top_share = float(shares_arr[0])
        second_share = float(shares_arr[1]) if len(shares_arr) > 1 else 0.0
        dominance_ratio = top_share / second_share if second_share > 0 else float("inf")

        # Multi-homing rate: low multi-homing + high concentration = tipping
        mh_rates = [p["multi_homing_rate"] for p in platforms if p["multi_homing_rate"] is not None]
        avg_multi_homing = float(np.mean(mh_rates)) if mh_rates else None

        # Switching costs
        sw_costs = [p["switching_cost"] for p in platforms if p["switching_cost"] is not None]
        avg_switching_cost = float(np.mean(sw_costs)) if sw_costs else None

        # Tipping index: composite measure
        tipping_score = 0.0
        if top_share > 0.5:
            tipping_score += 0.3
        if dominance_ratio > 3.0:
            tipping_score += 0.2
        if avg_multi_homing is not None and avg_multi_homing < 0.3:
            tipping_score += 0.25
        if avg_switching_cost is not None and avg_switching_cost > 0.5:
            tipping_score += 0.25

        return {
            "top_platform_share": round(top_share, 4),
            "dominance_ratio": round(dominance_ratio, 2) if dominance_ratio < 1e6 else None,
            "avg_multi_homing_rate": round(avg_multi_homing, 4) if avg_multi_homing is not None else None,
            "avg_switching_cost": round(avg_switching_cost, 4) if avg_switching_cost is not None else None,
            "tipping_index": round(tipping_score, 4),
            "market_tipped": top_share > 0.7 and (avg_multi_homing or 0) < 0.2,
        }

    @staticmethod
    def _regulation_assessment(platforms: list[dict]) -> dict | None:
        """Assess potential effects of platform regulation interventions."""
        shares = [p["market_share"] for p in platforms if p["market_share"] is not None]
        if len(shares) < 2:
            return None

        shares_arr = np.array(sorted(shares, reverse=True))
        shares_arr = shares_arr / shares_arr.sum() if shares_arr.sum() > 0 else shares_arr
        hhi = float(np.sum(shares_arr ** 2))

        # Interoperability mandate: reduces network effect lock-in
        # Simulated effect: HHI reduction proportional to network effect strength
        interop_hhi_reduction = min(hhi * 0.3, 0.15)

        # Price cap impact: if one side subsidized, cap on other side harms subsidy
        prices_a = [p["price_side_a"] for p in platforms if p["price_side_a"] is not None]
        prices_b = [p["price_side_b"] for p in platforms if p["price_side_b"] is not None]

        price_cap_risk = "low"
        if prices_a and prices_b:
            mean_a = np.mean(prices_a)
            mean_b = np.mean(prices_b)
            if mean_a > 0 and mean_b > 0:
                ratio = max(mean_a, mean_b) / min(mean_a, mean_b)
                if ratio > 5.0:
                    price_cap_risk = "high"
                elif ratio > 2.0:
                    price_cap_risk = "medium"

        # Data portability: reduces switching cost barrier
        sw_costs = [p["switching_cost"] for p in platforms if p["switching_cost"] is not None]
        portability_benefit = float(np.mean(sw_costs) * 0.5) if sw_costs else None

        return {
            "current_hhi": round(hhi, 4),
            "interoperability_hhi_reduction": round(interop_hhi_reduction, 4),
            "post_interop_hhi": round(hhi - interop_hhi_reduction, 4),
            "price_cap_risk": price_cap_risk,
            "data_portability_benefit": round(portability_benefit, 4) if portability_benefit is not None else None,
        }
