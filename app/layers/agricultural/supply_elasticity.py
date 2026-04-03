"""Nerlove partial adjustment model for agricultural supply response.

Estimates short-run and long-run supply elasticities for major crops using
the Nerlove (1958) framework. Farmers form adaptive expectations about
future prices based on past prices, and adjust acreage/output partially
toward their desired level each period due to adjustment costs (fixed
inputs, contracts, biological lags).

Model specification:
    A_t = b0 + b1*P*_t + b2*Z_t + u_t          (desired acreage)
    P*_t = P*_{t-1} + g*(P_{t-1} - P*_{t-1})    (adaptive expectations)
    A_t - A_{t-1} = d*(A*_t - A_{t-1})           (partial adjustment)

Reduced form (estimable):
    A_t = c0 + c1*P_{t-1} + c2*A_{t-1} + c3*Z_t + e_t

where:
    - Short-run elasticity = c1 (at means)
    - Adjustment coefficient d = 1 - c2
    - Long-run elasticity = c1 / (1 - c2) = c1 / d
    - Expectation coefficient g derived from structural parameters

Crops: wheat, rice, maize, soybeans.

Score (0-100): Higher score indicates slower supply adjustment (large gap
between short-run and long-run elasticities), signaling structural rigidity
in agricultural markets.

References:
    Nerlove, M. (1958). "The dynamics of supply: Estimation of farmer's
        response to price." Johns Hopkins University Press.
    Askari, H., Cummings, J.T. (1977). "Estimating agricultural supply
        response with the Nerlove model." International Economic Review.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class SupplyElasticity(LayerBase):
    layer_id = "l5"
    name = "Supply Elasticity"

    CROPS = ("wheat", "rice", "maize", "soybeans")

    async def compute(self, db, **kwargs) -> dict:
        """Estimate Nerlove partial adjustment supply elasticities.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default BGD)
            crops : list[str] - crop names to analyze
        """
        country = kwargs.get("country_iso3", "BGD")
        crops = kwargs.get("crops", self.CROPS)

        crop_results = {}
        valid_adjustments = []

        for crop in crops:
            # Fetch annual acreage and price series
            acreage_rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.source = 'fao'
                  AND ds.name LIKE ?
                  AND ds.unit = 'ha'
                ORDER BY dp.date ASC
                """,
                (country, f"%{crop}%area%"),
            )
            price_rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.source IN ('fao', 'wb_commodity')
                  AND ds.name LIKE ?
                  AND ds.unit LIKE '%USD%'
                ORDER BY dp.date ASC
                """,
                (country, f"%{crop}%price%"),
            )

            if len(acreage_rows) < 8 or len(price_rows) < 8:
                crop_results[crop] = {"status": "insufficient_data"}
                continue

            # Align series by date
            acreage_by_date = {r["date"]: r["value"] for r in acreage_rows}
            price_by_date = {r["date"]: r["value"] for r in price_rows}
            common_dates = sorted(set(acreage_by_date) & set(price_by_date))

            if len(common_dates) < 8:
                crop_results[crop] = {"status": "insufficient_overlap"}
                continue

            acreage = np.array([acreage_by_date[d] for d in common_dates], dtype=float)
            prices = np.array([price_by_date[d] for d in common_dates], dtype=float)

            # Remove zeros/negatives
            valid = (acreage > 0) & (prices > 0)
            acreage = acreage[valid]
            prices = prices[valid]

            if len(acreage) < 8:
                crop_results[crop] = {"status": "insufficient_valid_obs"}
                continue

            result = self._estimate_nerlove(acreage, prices)
            crop_results[crop] = result

            if result.get("adjustment_coeff") is not None:
                valid_adjustments.append(result["adjustment_coeff"])

        # Score: slow adjustment -> high score
        # adjustment_coeff near 0 = very slow (score near 100)
        # adjustment_coeff near 1 = instant (score near 0)
        if valid_adjustments:
            mean_adj = float(np.mean(valid_adjustments))
            score = max(0.0, min(100.0, (1.0 - mean_adj) * 100.0))
        else:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data for any crop",
            }

        return {
            "score": round(score, 2),
            "country": country,
            "crops": crop_results,
            "mean_adjustment_speed": round(float(np.mean(valid_adjustments)), 4),
            "n_crops_estimated": len(valid_adjustments),
        }

    @staticmethod
    def _estimate_nerlove(acreage: np.ndarray, prices: np.ndarray) -> dict:
        """Estimate Nerlove reduced-form and recover structural parameters.

        Reduced form: A_t = c0 + c1*P_{t-1} + c2*A_{t-1} + e_t

        Uses OLS with HC1 standard errors.
        """
        len(acreage)
        # Dependent variable: A_t (from t=1 onward)
        A_t = acreage[1:]
        # Regressors: lagged price P_{t-1}, lagged acreage A_{t-1}
        P_lag = prices[:-1]
        A_lag = acreage[:-1]
        n = len(A_t)

        # Compute means for elasticity at means
        mean_A = float(np.mean(A_t))
        mean_P = float(np.mean(P_lag))

        # Design matrix: [const, P_{t-1}, A_{t-1}]
        X = np.column_stack([np.ones(n), P_lag, A_lag])

        # OLS
        try:
            beta = np.linalg.lstsq(X, A_t, rcond=None)[0]
        except np.linalg.LinAlgError:
            return {"status": "estimation_failed"}

        c0, c1, c2 = beta

        resid = A_t - X @ beta
        ss_res = float(np.sum(resid ** 2))
        ss_tot = float(np.sum((A_t - np.mean(A_t)) ** 2))
        r_squared = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

        # HC1 standard errors
        k = X.shape[1]
        XtX_inv = np.linalg.inv(X.T @ X)
        omega = np.diag(resid ** 2) * (n / max(n - k, 1))
        V = XtX_inv @ (X.T @ omega @ X) @ XtX_inv
        se = np.sqrt(np.maximum(np.diag(V), 0.0))

        # Structural parameters
        # Adjustment coefficient: d = 1 - c2
        d = 1.0 - c2
        d = max(0.01, min(d, 1.0))  # bound to valid range

        # Short-run price elasticity (at means)
        sr_elasticity = c1 * (mean_P / mean_A) if mean_A > 0 else 0.0

        # Long-run price elasticity
        lr_elasticity = sr_elasticity / d if d > 0.01 else sr_elasticity * 100.0

        # Durbin-Watson statistic for serial correlation check
        if n > 1:
            dw = float(np.sum(np.diff(resid) ** 2) / ss_res) if ss_res > 0 else 2.0
        else:
            dw = 2.0

        return {
            "status": "ok",
            "n_obs": int(n),
            "coefficients": {
                "constant": round(float(c0), 6),
                "lagged_price": round(float(c1), 6),
                "lagged_acreage": round(float(c2), 6),
            },
            "std_errors": {
                "constant": round(float(se[0]), 6),
                "lagged_price": round(float(se[1]), 6),
                "lagged_acreage": round(float(se[2]), 6),
            },
            "r_squared": round(r_squared, 4),
            "durbin_watson": round(dw, 4),
            "adjustment_coeff": round(float(d), 4),
            "sr_elasticity": round(float(sr_elasticity), 4),
            "lr_elasticity": round(float(lr_elasticity), 4),
            "mean_acreage": round(mean_A, 2),
            "mean_price": round(mean_P, 2),
        }
