"""Inverse farm size-productivity relationship testing.

Tests the widely-documented inverse relationship (IR) between farm size and
land productivity, accounting for selection bias and omitted variables.
Distinguishes between supervision-cost advantages of small farms and
transaction-cost disadvantages.

Methodology:
    1. Naive IR test: regress log yield on log farm size
       ln(y_i) = alpha + beta*ln(A_i) + e_i
       Negative beta = inverse relationship holds.

    2. Augmented specification with controls:
       ln(y_i) = alpha + beta*ln(A_i) + gamma*X_i + e_i
       where X includes soil quality, irrigation access, input use,
       household characteristics.

    3. Selection bias correction (Heckman two-step):
       Stage 1: Probit of participation in market P(sell=1|Z)
       Stage 2: Include inverse Mills ratio lambda in yield equation
       to control for selection into market participation.

    4. Quantile regression to test if IR varies across yield distribution.

    5. Supervision vs transaction cost decomposition:
       - Small farms: higher labor supervision per hectare (Sen 1962)
       - Large farms: lower transaction costs per unit output
       Net effect depends on labor market imperfections and infrastructure.

    Score: strong IR without institutional support = high vulnerability of
    smallholders to consolidation pressure.

References:
    Sen, A.K. (1962). "An Aspect of Indian Agriculture." Economic Weekly.
    Berry, R.A. & Cline, W.R. (1979). "Agrarian Structure and Productivity
        in Developing Countries." Johns Hopkins.
    Barrett, C.B., Bellemare, M.F. & Hou, J.Y. (2010). "Reconsidering
        conventional explanations of the inverse productivity-size
        relationship." World Development, 38(1), 88-97.
    Heckman, J.J. (1979). "Sample Selection Bias as a Specification Error."
        Econometrica, 47(1), 153-161.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class FarmSizeProductivity(LayerBase):
    layer_id = "l5"
    name = "Farm Size-Productivity"

    async def compute(self, db, **kwargs) -> dict:
        """Test inverse farm size-productivity relationship.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
            crop : str - crop filter
        """
        country = kwargs.get("country_iso3", "BGD")
        crop = kwargs.get("crop")

        crop_clause = "AND ds.description LIKE '%' || ? || '%'" if crop else ""
        params = [country]
        if crop:
            params.append(crop)

        rows = await db.fetch_all(
            f"""
            SELECT dp.value AS yield_val, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source = 'farm_survey'
              AND ds.country_iso3 = ?
              {crop_clause}
            ORDER BY dp.date
            """,
            tuple(params),
        )

        if not rows or len(rows) < 20:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient farm survey data"}

        import json

        yields = []
        farm_sizes = []
        soil_quals = []
        irrig_access = []
        fert_use = []
        labor_days = []
        market_participant = []
        hh_size = []

        for row in rows:
            y_val = row["yield_val"]
            if y_val is None or y_val <= 0:
                continue
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            area = meta.get("farm_size_ha")
            if area is None or area <= 0:
                continue

            yields.append(float(y_val))
            farm_sizes.append(float(area))
            soil_quals.append(float(meta.get("soil_quality_index", 50)))
            irrig_access.append(float(meta.get("irrigation_access", 0)))
            fert_use.append(float(meta.get("fertilizer_kg_ha", 0)))
            labor_days.append(float(meta.get("labor_days_ha", 0)))
            market_participant.append(int(meta.get("sells_to_market", 1)))
            hh_size.append(float(meta.get("household_size", 5)))

        n = len(yields)
        if n < 20:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient valid observations"}

        Y = np.log(np.array(yields))
        A = np.log(np.array(farm_sizes))
        soil = np.array(soil_quals)
        irrig = np.array(irrig_access)
        fert = np.array(fert_use)
        labor = np.array(labor_days)
        market = np.array(market_participant)
        hh = np.array(hh_size)

        # 1. Naive IR regression: ln(y) = a + b*ln(A) + e
        X_naive = np.column_stack([np.ones(n), A])
        beta_naive = np.linalg.lstsq(X_naive, Y, rcond=None)[0]
        fitted_naive = X_naive @ beta_naive
        resid_naive = Y - fitted_naive
        ss_res = float(np.sum(resid_naive ** 2))
        ss_tot = float(np.sum((Y - Y.mean()) ** 2))
        r2_naive = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

        se_naive = self._robust_se(X_naive, resid_naive)
        t_naive = beta_naive[1] / se_naive[1] if se_naive[1] > 0 else 0.0

        # 2. Augmented regression with controls
        controls = []
        control_names = []
        for arr, name in [
            (soil, "soil_quality"), (irrig, "irrigation"),
            (fert, "fertilizer"), (labor, "labor"),
        ]:
            if arr.std() > 1e-8:
                controls.append(arr)
                control_names.append(name)

        if controls:
            X_aug = np.column_stack([np.ones(n), A] + controls)
        else:
            X_aug = X_naive

        beta_aug = np.linalg.lstsq(X_aug, Y, rcond=None)[0]
        fitted_aug = X_aug @ beta_aug
        resid_aug = Y - fitted_aug
        ss_res_aug = float(np.sum(resid_aug ** 2))
        r2_aug = 1.0 - ss_res_aug / ss_tot if ss_tot > 0 else 0.0

        se_aug = self._robust_se(X_aug, resid_aug)
        t_aug = beta_aug[1] / se_aug[1] if se_aug[1] > 0 else 0.0

        coef_names_aug = ["constant", "ln_farm_size"] + control_names
        aug_coefs = dict(zip(coef_names_aug, beta_aug.tolist()))
        aug_se = dict(zip(coef_names_aug, se_aug.tolist()))

        # 3. Heckman selection correction (simplified two-step)
        heckman_result = None
        if market.std() > 0 and market.mean() < 0.95:
            heckman_result = self._heckman_two_step(
                Y, A, controls, market, hh
            )

        # 4. Quantile regression at 25th, 50th, 75th percentiles
        quantile_results = {}
        for q in [0.25, 0.50, 0.75]:
            qr = self._quantile_regression(X_naive, Y, q)
            if qr is not None:
                quantile_results[f"q{int(q*100)}"] = {
                    "ln_farm_size_coef": round(float(qr[1]), 4),
                }

        # 5. Farm size distribution statistics
        farm_arr = np.array(farm_sizes)
        gini = self._gini_coefficient(farm_arr)
        size_classes = {
            "marginal_lt_0.5ha": float((farm_arr < 0.5).mean()),
            "small_0.5_2ha": float(((farm_arr >= 0.5) & (farm_arr < 2)).mean()),
            "medium_2_10ha": float(((farm_arr >= 2) & (farm_arr < 10)).mean()),
            "large_gt_10ha": float((farm_arr >= 10).mean()),
        }

        # Supervision intensity proxy: labor per hectare by farm size
        labor_arr = np.array(labor_days)
        small_mask = farm_arr < np.median(farm_arr)
        supervision_ratio = None
        if small_mask.sum() > 0 and (~small_mask).sum() > 0:
            labor_per_ha_small = labor_arr[small_mask].mean() / farm_arr[small_mask].mean()
            labor_per_ha_large = labor_arr[~small_mask].mean() / farm_arr[~small_mask].mean()
            if labor_per_ha_large > 0:
                supervision_ratio = labor_per_ha_small / labor_per_ha_large

        # Score: strong IR + high land inequality + many marginal farmers = stress
        ir_strength = abs(min(float(beta_naive[1]), 0))  # magnitude of negative slope
        ir_score = float(np.clip(ir_strength * 50, 0, 40))
        ineq_score = float(np.clip(gini * 60, 0, 30))
        marginal_score = float(np.clip(size_classes["marginal_lt_0.5ha"] * 60, 0, 30))
        score = float(np.clip(ir_score + ineq_score + marginal_score, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "n_obs": n,
            "naive_regression": {
                "ln_farm_size_coef": round(float(beta_naive[1]), 4),
                "std_error": round(float(se_naive[1]), 4),
                "t_statistic": round(t_naive, 2),
                "r_squared": round(r2_naive, 4),
                "inverse_relationship": bool(beta_naive[1] < 0 and abs(t_naive) > 1.96),
            },
            "augmented_regression": {
                "coefficients": {k: round(v, 4) for k, v in aug_coefs.items()},
                "std_errors": {k: round(v, 4) for k, v in aug_se.items()},
                "t_statistic_farm_size": round(t_aug, 2),
                "r_squared": round(r2_aug, 4),
                "ir_survives_controls": bool(beta_aug[1] < 0 and abs(t_aug) > 1.96),
            },
            "heckman_correction": heckman_result,
            "quantile_regression": quantile_results,
            "farm_distribution": {
                "mean_ha": round(float(farm_arr.mean()), 2),
                "median_ha": round(float(np.median(farm_arr)), 2),
                "gini_land": round(float(gini), 4),
                "size_class_shares": {k: round(v, 3) for k, v in size_classes.items()},
            },
            "supervision_labor_ratio_small_to_large": (
                round(float(supervision_ratio), 2) if supervision_ratio is not None else None
            ),
        }

    @staticmethod
    def _robust_se(X: np.ndarray, resid: np.ndarray) -> np.ndarray:
        """HC1 heteroskedasticity-robust standard errors."""
        n, k = X.shape
        XtX_inv = np.linalg.pinv(X.T @ X)
        scale = n / max(n - k, 1)
        omega = np.diag(resid ** 2) * scale
        V = XtX_inv @ (X.T @ omega @ X) @ XtX_inv
        return np.sqrt(np.maximum(np.diag(V), 0.0))

    @staticmethod
    def _gini_coefficient(x: np.ndarray) -> float:
        """Gini coefficient of inequality."""
        x_sorted = np.sort(x)
        n = len(x_sorted)
        if n == 0 or x_sorted.sum() == 0:
            return 0.0
        index = np.arange(1, n + 1)
        return float((2 * np.sum(index * x_sorted) - (n + 1) * np.sum(x_sorted)) / (n * np.sum(x_sorted)))

    @staticmethod
    def _quantile_regression(
        X: np.ndarray, y: np.ndarray, tau: float
    ) -> np.ndarray | None:
        """Quantile regression via iteratively reweighted least squares.

        Minimizes sum of check function: rho_tau(u) = u * (tau - I(u < 0)).
        """
        try:
            n, k = X.shape
            beta = np.linalg.lstsq(X, y, rcond=None)[0]

            for _ in range(100):
                resid = y - X @ beta
                weights = np.where(resid >= 0, tau, 1 - tau)
                weights = np.maximum(weights / (np.abs(resid) + 1e-8), 1e-8)

                W = np.diag(weights)
                try:
                    beta_new = np.linalg.solve(X.T @ W @ X, X.T @ W @ y)
                except np.linalg.LinAlgError:
                    break
                if np.max(np.abs(beta_new - beta)) < 1e-6:
                    return beta_new
                beta = beta_new
            return beta
        except Exception:
            return None

    @staticmethod
    def _heckman_two_step(
        Y: np.ndarray, A: np.ndarray, controls: list[np.ndarray],
        selection: np.ndarray, instruments: np.ndarray,
    ) -> dict | None:
        """Simplified Heckman two-step selection correction.

        Stage 1: Probit of market participation on instruments.
        Stage 2: OLS of yield including inverse Mills ratio.
        """
        try:
            n = len(Y)
            # Stage 1: approximate probit with logistic regression
            Z = np.column_stack([np.ones(n), A, instruments])
            # Logistic regression via IRLS
            gamma = np.zeros(Z.shape[1])
            for _ in range(50):
                p = 1 / (1 + np.exp(-Z @ gamma))
                p = np.clip(p, 1e-8, 1 - 1e-8)
                W = p * (1 - p)
                z_star = Z @ gamma + (selection - p) / W
                try:
                    gamma_new = np.linalg.solve(Z.T @ (Z * W[:, None]), Z.T @ (W * z_star))
                except np.linalg.LinAlgError:
                    break
                if np.max(np.abs(gamma_new - gamma)) < 1e-6:
                    gamma = gamma_new
                    break
                gamma = gamma_new

            p_hat = 1 / (1 + np.exp(-Z @ gamma))
            p_hat = np.clip(p_hat, 1e-8, 1 - 1e-8)

            # Inverse Mills ratio (approximation using logistic)
            # For probit: lambda = phi(Zg) / Phi(Zg)
            # Logistic approximation: lambda ~ p * (1-p) / p = (1-p)
            from scipy.stats import norm
            Zg = Z @ gamma
            imr = norm.pdf(Zg) / np.maximum(norm.cdf(Zg), 1e-8)

            # Stage 2: augmented OLS
            X_heck = np.column_stack([np.ones(n), A] + controls + [imr])
            beta_heck = np.linalg.lstsq(X_heck, Y, rcond=None)[0]

            # Test significance of IMR (selection bias)
            fitted = X_heck @ beta_heck
            resid = Y - fitted
            se_heck = FarmSizeProductivity._robust_se(X_heck, resid)
            imr_coef = float(beta_heck[-1])
            imr_se = float(se_heck[-1])
            imr_t = imr_coef / imr_se if imr_se > 0 else 0.0

            return {
                "ln_farm_size_coef_corrected": round(float(beta_heck[1]), 4),
                "imr_coefficient": round(imr_coef, 4),
                "imr_t_statistic": round(imr_t, 2),
                "selection_bias_significant": bool(abs(imr_t) > 1.96),
            }
        except Exception:
            return None
