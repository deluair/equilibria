"""Merger analysis: HHI delta, upward pricing pressure, and merger simulation.

Horizontal merger screening follows DOJ/FTC (2023) guidelines:

    Delta HHI = HHI_post - HHI_pre

    When merging firms with shares s1 and s2:
        Delta HHI = 2 * s1 * s2

Thresholds (DOJ/FTC):
    HHI_post < 0.15: unlikely to raise concerns
    0.15 <= HHI_post < 0.25 AND Delta_HHI > 0.01: potentially concerning
    HHI_post >= 0.25 AND Delta_HHI > 0.015: presumed anticompetitive

Upward Pricing Pressure (UPP) from Farrell & Shapiro (2010) tests whether
merged firm has incentive to raise prices without full simulation:

    UPP_1 = D_{12} * (P_2 - MC_2) - E * MC_1

where D_{12} is the diversion ratio from product 1 to 2, and E is a
default efficiency credit (typically 10%).

Merger simulation with logit demand (Werden & Froeb 1994) computes
post-merger equilibrium prices under the assumption of Nash-Bertrand
competition with logit demand:

    s_j = exp(delta_j - alpha*p_j) / sum_k exp(delta_k - alpha*p_k)

where alpha is price sensitivity and delta_j is mean utility.

References:
    DOJ/FTC (2023). Merger Guidelines.
    Farrell, J. & Shapiro, C. (2010). Antitrust Evaluation of Horizontal
        Mergers. B.E. Journal of Theoretical Economics 10(1).
    Werden, G. & Froeb, L. (1994). The Effects of Mergers in
        Differentiated Products Industries. Journal of Law, Economics
        & Organization 10(2): 407-426.

Score: high UPP or large HHI delta -> STRESS/CRISIS, benign merger -> STABLE.
"""

import json

import numpy as np
from scipy.optimize import fsolve

from app.layers.base import LayerBase


class MergerAnalysis(LayerBase):
    layer_id = "l14"
    name = "Merger Analysis"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")
        year = kwargs.get("year")

        year_clause = "AND dp.date = ?" if year else ""
        params: list = [country, "merger_analysis"]
        if year:
            params.append(str(year))

        rows = await db.fetch_all(
            f"""
            SELECT dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = ?
              {year_clause}
            ORDER BY dp.date DESC
            """,
            tuple(params),
        )

        if not rows or len(rows) < 4:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient merger data"}

        shares = []
        prices = []
        marginal_costs = []
        merging_pair = None

        for row in rows:
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            share = meta.get("market_share")
            price = meta.get("price")
            mc = meta.get("marginal_cost")
            is_merging = meta.get("merging", False)

            if share is not None:
                firm_data = {
                    "share": float(share),
                    "price": float(price) if price is not None else None,
                    "mc": float(mc) if mc is not None else None,
                    "merging": bool(is_merging),
                }
                shares.append(firm_data)

        if len(shares) < 4:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient firms"}

        merging_firms = [f for f in shares if f["merging"]]
        if len(merging_firms) < 2:
            merging_firms = sorted(shares, key=lambda x: -x["share"])[:2]

        all_shares = np.array([f["share"] for f in shares])
        all_shares = all_shares / all_shares.sum() if all_shares.sum() > 0 else all_shares

        s1 = merging_firms[0]["share"]
        s2 = merging_firms[1]["share"]
        total = all_shares.sum()
        s1_norm = s1 / total if total > 0 else s1
        s2_norm = s2 / total if total > 0 else s2

        # Pre-merger HHI
        hhi_pre = float(np.sum(all_shares ** 2))

        # Delta HHI (exact formula for two merging firms)
        delta_hhi = 2.0 * s1_norm * s2_norm

        # Post-merger HHI
        hhi_post = hhi_pre + delta_hhi

        # DOJ/FTC screening
        if hhi_post < 0.15:
            screening = "unlikely to raise concerns"
        elif hhi_post < 0.25 and delta_hhi > 0.01:
            screening = "potentially concerning"
        elif hhi_post >= 0.25 and delta_hhi > 0.015:
            screening = "presumed anticompetitive"
        else:
            screening = "below threshold"

        # Diversion ratios (proportional to share, logit assumption)
        # D_{12} = s2 / (1 - s1)
        d_12 = s2_norm / (1.0 - s1_norm) if s1_norm < 1.0 else 1.0
        d_21 = s1_norm / (1.0 - s2_norm) if s2_norm < 1.0 else 1.0

        # Upward Pricing Pressure (UPP)
        efficiency_credit = kwargs.get("efficiency_credit", 0.10)
        upp_1 = None
        upp_2 = None
        if merging_firms[0]["price"] is not None and merging_firms[1]["mc"] is not None:
            p2 = merging_firms[1]["price"]
            mc2 = merging_firms[1]["mc"]
            mc1 = merging_firms[0]["mc"] or merging_firms[0]["price"] * 0.6
            margin_2 = p2 - mc2
            upp_1 = d_12 * margin_2 - efficiency_credit * mc1

        if merging_firms[1]["price"] is not None and merging_firms[0]["mc"] is not None:
            p1 = merging_firms[0]["price"]
            mc1 = merging_firms[0]["mc"]
            mc2 = merging_firms[1]["mc"] or merging_firms[1]["price"] * 0.6
            margin_1 = p1 - mc1
            upp_2 = d_21 * margin_1 - efficiency_credit * mc2

        # Merger simulation: logit demand
        simulation = self._logit_merger_simulation(shares, merging_firms, kwargs.get("alpha", 1.0))

        # Score: combine delta HHI and UPP signals
        # delta_hhi > 0.02 is serious, > 0.05 is critical
        hhi_score = min(delta_hhi / 0.05 * 75.0, 75.0)

        upp_score = 0.0
        if upp_1 is not None and upp_1 > 0:
            upp_score = min(upp_1 / (merging_firms[0].get("price", 1.0) or 1.0) * 100.0, 25.0)

        score = max(0.0, min(100.0, hhi_score + upp_score))

        return {
            "score": round(score, 2),
            "country": country,
            "n_firms": len(shares),
            "hhi_pre": round(hhi_pre, 4),
            "hhi_post": round(hhi_post, 4),
            "delta_hhi": round(delta_hhi, 4),
            "screening": screening,
            "diversion_ratios": {
                "d_12": round(d_12, 4),
                "d_21": round(d_21, 4),
            },
            "upp": {
                "product_1": round(upp_1, 4) if upp_1 is not None else None,
                "product_2": round(upp_2, 4) if upp_2 is not None else None,
                "efficiency_credit": efficiency_credit,
            },
            "simulation": simulation,
        }

    @staticmethod
    def _logit_merger_simulation(
        firms: list[dict], merging: list[dict], alpha: float
    ) -> dict | None:
        """Simulate post-merger prices under logit demand / Bertrand competition.

        Each firm j has share s_j, price p_j, marginal cost mc_j.
        Pre-merger FOC: p_j - mc_j = 1 / (alpha * (1 - s_j))
        Post-merger: merging firms internalize cross-price effects.
        """
        has_prices = all(f.get("price") is not None and f.get("mc") is not None for f in firms)
        if not has_prices or alpha <= 0:
            return None

        n = len(firms)
        prices_pre = np.array([f["price"] for f in firms])
        mc = np.array([f["mc"] for f in firms])
        shares_pre = np.array([f["share"] for f in firms])
        shares_pre = shares_pre / shares_pre.sum()

        merging_indices = set()
        for mf in merging:
            for i, f in enumerate(firms):
                if abs(f["share"] - mf["share"]) < 1e-10:
                    merging_indices.add(i)
                    break

        # Mean utility: delta_j = log(s_j / s_0) + alpha * p_j
        # Use outside good share s_0 = 0.2
        s0 = 0.2
        delta = np.log(shares_pre / s0) + alpha * prices_pre

        def post_merger_foc(p):
            """First-order conditions for post-merger Bertrand-Nash."""
            exp_v = np.exp(delta - alpha * p)
            denom = s0 + np.sum(exp_v)
            s = exp_v / denom
            residuals = np.zeros(n)
            for j in range(n):
                own_markup = p[j] - mc[j]
                # Single-product Bertrand: markup = 1 / (alpha * (1 - s_j))
                if j in merging_indices:
                    # Merged firm internalizes cannibalization
                    partner_margin = sum(
                        (p[k] - mc[k]) * s[k] for k in merging_indices if k != j
                    )
                    residuals[j] = own_markup - (1.0 + alpha * partner_margin) / (alpha * (1.0 - s[j]))
                else:
                    residuals[j] = own_markup - 1.0 / (alpha * (1.0 - s[j]))
            return residuals

        try:
            prices_post = fsolve(post_merger_foc, prices_pre, full_output=False)
            price_changes = (prices_post - prices_pre) / prices_pre * 100.0

            exp_v_post = np.exp(delta - alpha * prices_post)
            denom_post = s0 + np.sum(exp_v_post)
            shares_post = exp_v_post / denom_post

            return {
                "price_changes_pct": {
                    f"firm_{i}": round(float(price_changes[i]), 2) for i in range(n)
                },
                "share_changes": {
                    f"firm_{i}": round(float(shares_post[i] - shares_pre[i]), 4) for i in range(n)
                },
                "avg_price_increase_pct": round(float(np.mean(price_changes)), 2),
                "max_price_increase_pct": round(float(np.max(price_changes)), 2),
            }
        except Exception:
            return None
