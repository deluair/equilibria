"""Almost Ideal Demand System (AIDS) for food expenditure analysis.

Estimates the Deaton & Muellbauer (1980) AIDS model, which expresses budget
shares as functions of prices and total expenditure. The AIDS has a flexible
functional form consistent with aggregation over consumers and nests many
common demand specifications.

AIDS budget share equations:
    w_i = a_i + sum_j(g_ij * ln(p_j)) + b_i * ln(X / P)

where:
    w_i = budget share of good i
    p_j = price of good j
    X = total food expenditure
    P = Stone price index: ln(P) = sum_k(w_k * ln(p_k))
    a_i, g_ij, b_i = parameters

Restrictions:
    Adding-up:  sum_i(a_i)=1, sum_i(g_ij)=0, sum_i(b_i)=0
    Homogeneity: sum_j(g_ij) = 0 for all i
    Symmetry:   g_ij = g_ji

Elasticities derived:
    - Marshallian (uncompensated) price elasticity
    - Hicksian (compensated) price elasticity (via Slutsky equation)
    - Expenditure elasticity

EASI extension (Lewbel & Pendakur 2009): adds polynomial terms in real
expenditure for nonlinear Engel curves. Approximated here via quadratic
budget share terms.

Score (0-100): Higher score indicates larger food expenditure shares
concentrated in staples, signaling food insecurity / lack of dietary
diversification.

References:
    Deaton, A., Muellbauer, J. (1980). "An Almost Ideal Demand System."
        American Economic Review, 70(3), 312-326.
    Lewbel, A., Pendakur, K. (2009). "Tricks with Hicks: The EASI demand
        system." American Economic Review, 99(3), 827-863.
"""

from __future__ import annotations

import numpy as np
from app.layers.base import LayerBase


class DemandSystem(LayerBase):
    layer_id = "l5"
    name = "Demand System (AIDS)"

    FOOD_GROUPS = ("cereals", "meat", "dairy", "fruits_veg", "oils_fats")

    async def compute(self, db, **kwargs) -> dict:
        """Estimate AIDS model for food demand.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code
            food_groups : tuple - food group names
        """
        country = kwargs.get("country_iso3", "BGD")
        food_groups = kwargs.get("food_groups", self.FOOD_GROUPS)
        n_goods = len(food_groups)

        # Fetch expenditure share and price data by food group
        shares_data = {}
        prices_data = {}
        all_dates = set()

        for fg in food_groups:
            share_rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.source IN ('fao', 'hces', 'wb')
                  AND ds.name LIKE ?
                  AND ds.unit = 'share'
                ORDER BY dp.date ASC
                """,
                (country, f"%{fg}%budget%share%"),
            )
            price_rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.source IN ('fao', 'wb_commodity', 'cpi')
                  AND ds.name LIKE ?
                  AND ds.unit LIKE '%index%'
                ORDER BY dp.date ASC
                """,
                (country, f"%{fg}%price%"),
            )

            if share_rows:
                shares_data[fg] = {r["date"]: r["value"] for r in share_rows}
                all_dates.update(shares_data[fg].keys())
            if price_rows:
                prices_data[fg] = {r["date"]: r["value"] for r in price_rows}
                all_dates.update(prices_data[fg].keys())

        # Find dates common to all series
        valid_groups = [fg for fg in food_groups if fg in shares_data and fg in prices_data]
        if len(valid_groups) < 3:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"need >= 3 food groups, got {len(valid_groups)}",
            }

        common_dates = sorted(all_dates)
        for fg in valid_groups:
            common_dates = [d for d in common_dates if d in shares_data[fg] and d in prices_data[fg]]

        T = len(common_dates)
        if T < 10:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"insufficient time periods: {T}",
            }

        n_goods = len(valid_groups)

        # Build matrices
        W = np.zeros((T, n_goods))  # budget shares
        P = np.zeros((T, n_goods))  # log prices

        for j, fg in enumerate(valid_groups):
            for t, date in enumerate(common_dates):
                W[t, j] = shares_data[fg][date]
                P[t, j] = np.log(max(prices_data[fg][date], 0.01))

        # Total expenditure proxy: sum of shares should be ~1, but use index
        # Fetch total food expenditure or use shares sum
        total_exp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source IN ('wb', 'hces')
              AND ds.name LIKE '%food%expenditure%'
            ORDER BY dp.date ASC
            """,
            (country,),
        )
        exp_by_date = {r["date"]: r["value"] for r in total_exp_rows} if total_exp_rows else {}

        # Use expenditure data if available, otherwise construct from prices
        if len(set(common_dates) & set(exp_by_date)) >= T * 0.5:
            ln_X = np.array([np.log(max(exp_by_date.get(d, 100.0), 1.0)) for d in common_dates])
        else:
            # Proxy: geometric mean of prices weighted by shares
            ln_X = np.sum(W * P, axis=1) + np.log(100.0)

        # Stone price index: ln(P*) = sum_k(w_k * ln(p_k))
        ln_P_stone = np.sum(W * P, axis=1)

        # Real expenditure
        ln_real_X = ln_X - ln_P_stone

        # Estimate AIDS equation for each good (drop last for adding-up)
        aids_results = {}
        all_elasticities = {}

        for i in range(n_goods - 1):
            w_i = W[:, i]
            # Regressors: constant, ln(p_1)...ln(p_n), ln(X/P)
            X_mat = np.column_stack([np.ones(T), P, ln_real_X])
            k = X_mat.shape[1]

            try:
                beta = np.linalg.lstsq(X_mat, w_i, rcond=None)[0]
            except np.linalg.LinAlgError:
                continue

            resid = w_i - X_mat @ beta
            ss_res = float(np.sum(resid ** 2))
            ss_tot = float(np.sum((w_i - np.mean(w_i)) ** 2))
            r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

            # Extract parameters
            alpha_i = beta[0]
            gamma_ij = beta[1 : 1 + n_goods]
            beta_i = beta[1 + n_goods]

            # Mean budget share
            w_bar = float(np.mean(w_i))

            # Marshallian own-price elasticity: e_ii = -1 + gamma_ii/w_i - beta_i
            marsh_own = -1.0 + gamma_ij[i] / w_bar - beta_i if w_bar > 0 else -1.0

            # Expenditure elasticity: eta_i = 1 + beta_i / w_i
            exp_elast = 1.0 + beta_i / w_bar if w_bar > 0 else 1.0

            # Hicksian own-price elasticity (Slutsky): e_ii^H = e_ii + eta_i * w_i
            hicks_own = marsh_own + exp_elast * w_bar

            # Cross-price elasticities (Marshallian)
            cross_elast = {}
            for j in range(n_goods):
                if j != i:
                    e_ij = gamma_ij[j] / w_bar - beta_i * float(np.mean(W[:, j])) / w_bar
                    cross_elast[valid_groups[j]] = round(float(e_ij), 4)

            aids_results[valid_groups[i]] = {
                "alpha": round(float(alpha_i), 6),
                "gamma": {valid_groups[j]: round(float(gamma_ij[j]), 6) for j in range(n_goods)},
                "beta": round(float(beta_i), 6),
                "r_squared": round(r2, 4),
                "mean_share": round(w_bar, 4),
            }

            all_elasticities[valid_groups[i]] = {
                "marshallian_own_price": round(float(marsh_own), 4),
                "hicksian_own_price": round(float(hicks_own), 4),
                "expenditure": round(float(exp_elast), 4),
                "cross_price": cross_elast,
            }

        # EASI extension: add quadratic term in real expenditure
        easi_results = {}
        for i in range(min(n_goods - 1, len(valid_groups))):
            w_i = W[:, i]
            X_easi = np.column_stack([np.ones(T), P, ln_real_X, ln_real_X ** 2])
            try:
                beta_easi = np.linalg.lstsq(X_easi, w_i, rcond=None)[0]
                resid_e = w_i - X_easi @ beta_easi
                ss_res_e = float(np.sum(resid_e ** 2))
                ss_tot_e = float(np.sum((w_i - np.mean(w_i)) ** 2))
                r2_easi = 1.0 - ss_res_e / ss_tot_e if ss_tot_e > 0 else 0.0
                easi_results[valid_groups[i]] = {
                    "quadratic_term": round(float(beta_easi[-1]), 6),
                    "r_squared_easi": round(r2_easi, 4),
                    "nonlinear_engel": abs(beta_easi[-1]) > 0.01,
                }
            except np.linalg.LinAlgError:
                pass

        # Score: high concentration in cereals/staples -> high score
        cereal_share = float(np.mean(W[:, 0])) if n_goods > 0 else 0.5
        # Herfindahl of food shares
        mean_shares = np.mean(W, axis=0)
        mean_shares = mean_shares / np.sum(mean_shares) if np.sum(mean_shares) > 0 else mean_shares
        hhi = float(np.sum(mean_shares ** 2))
        # HHI ranges from 1/n (equal) to 1 (concentrated)
        # Normalize: (HHI - 1/n) / (1 - 1/n) -> 0 to 1
        hhi_norm = (hhi - 1.0 / n_goods) / (1.0 - 1.0 / n_goods) if n_goods > 1 else 0.5
        score = max(0.0, min(100.0, hhi_norm * 70.0 + cereal_share * 30.0))

        return {
            "score": round(score, 2),
            "country": country,
            "n_goods": n_goods,
            "n_periods": T,
            "food_groups": valid_groups,
            "aids_parameters": aids_results,
            "elasticities": all_elasticities,
            "easi_extension": easi_results,
            "diagnostics": {
                "mean_budget_shares": {
                    valid_groups[j]: round(float(np.mean(W[:, j])), 4)
                    for j in range(n_goods)
                },
                "herfindahl_index": round(hhi, 4),
                "cereal_share": round(cereal_share, 4),
            },
        }
