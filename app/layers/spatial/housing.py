"""Housing economics analysis.

Hedonic price model, affordability indexing, bubble detection via price-rent
ratios, and spatial autoregression for house price spillovers.

Hedonic model (Rosen 1974):
    ln(P_i) = b0 + b1*sqft_i + b2*bedrooms_i + b3*age_i + b4*dist_cbd_i
              + b5*crime_i + b6*school_i + e_i

Price-rent ratio bubble detection:
    PRR = median_house_price / annual_rent
    Historical average ~15-16. Above 20 signals overvaluation (Himmelberg,
    Mayer & Sinai 2005). Above 25 is bubble territory.

Housing affordability index:
    HAI = (median_household_income * 0.28 * 12) / (annual_mortgage_payment)
    HAI > 1 means the median family can afford the median home.

Spatial autoregression (SAR) for house prices (Anselin 1988):
    P = rho * W * P + X * beta + epsilon
    where W is a spatial weights matrix (contiguity or distance-based)
    and rho captures spatial dependence in prices.

References:
    Rosen, S. (1974). Hedonic Prices and Implicit Markets. JPE 82(1).
    Himmelberg, C., Mayer, C. & Sinai, T. (2005). Assessing High House
        Prices: Bubbles, Fundamentals and Misperceptions. JEP 19(4).
    Anselin, L. (1988). Spatial Econometrics: Methods and Models.

Score: unaffordable housing + bubble signals + strong spatial contagion -> STRESS.
"""

import json

import numpy as np
from scipy import linalg as la

from app.layers.base import LayerBase


class HousingEconomics(LayerBase):
    layer_id = "l11"
    name = "Housing Economics"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        # --- Fetch housing data ---
        rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata, ds.description
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'housing'
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows or len(rows) < 10:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient housing data"}

        # Parse housing observations
        prices = []
        features = []
        rents = []
        incomes = []
        regions = []

        for row in rows:
            price = row["value"]
            if price is None or price <= 0:
                continue
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}

            sqft = meta.get("sqft")
            bedrooms = meta.get("bedrooms")
            age = meta.get("age")
            dist_cbd = meta.get("dist_cbd")
            if not all(v is not None for v in [sqft, bedrooms, age, dist_cbd]):
                continue
            if sqft <= 0 or dist_cbd <= 0:
                continue

            prices.append(price)
            features.append([
                1.0,
                float(sqft),
                float(bedrooms),
                float(age),
                np.log(float(dist_cbd)),
                float(meta.get("crime_rate", 0)),
                float(meta.get("school_quality", 0)),
            ])

            rent = meta.get("annual_rent")
            if rent and rent > 0:
                rents.append((price, rent))

            income = meta.get("household_income")
            if income and income > 0:
                incomes.append(income)

            region = meta.get("region")
            if region:
                regions.append(region)

        n = len(prices)
        if n < 10:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient valid obs"}

        y = np.log(np.array(prices))
        X = np.array(features)

        # --- Hedonic price model (OLS with HC1) ---
        beta = np.linalg.lstsq(X, y, rcond=None)[0]
        resid = y - X @ beta
        ss_res = float(np.sum(resid ** 2))
        ss_tot = float(np.sum((y - y.mean()) ** 2))
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

        k = X.shape[1]
        XtX_inv = np.linalg.inv(X.T @ X)
        omega = np.diag(resid ** 2) * (n / (n - k))
        V = XtX_inv @ (X.T @ omega @ X) @ XtX_inv
        se = np.sqrt(np.maximum(np.diag(V), 0.0))

        coef_names = [
            "constant", "sqft", "bedrooms", "age", "ln_dist_cbd",
            "crime_rate", "school_quality",
        ]

        # --- Price-rent ratio / bubble detection ---
        prr = None
        bubble_signal = "no_data"
        if len(rents) >= 5:
            prr_values = [p / r for p, r in rents]
            prr = float(np.median(prr_values))
            if prr > 25:
                bubble_signal = "bubble"
            elif prr > 20:
                bubble_signal = "overvalued"
            elif prr > 16:
                bubble_signal = "elevated"
            else:
                bubble_signal = "normal"

        # --- Housing affordability index ---
        hai = None
        if incomes and len(prices) >= 5:
            median_price = float(np.median(prices))
            median_income = float(np.median(incomes))
            # Assume 30-year mortgage at 6%, 20% down, 28% income threshold
            principal = median_price * 0.80
            monthly_rate = 0.06 / 12
            n_payments = 360
            if monthly_rate > 0:
                monthly_payment = principal * (
                    monthly_rate * (1 + monthly_rate) ** n_payments
                ) / ((1 + monthly_rate) ** n_payments - 1)
                annual_payment = monthly_payment * 12
                affordable_payment = median_income * 0.28
                hai = float(affordable_payment / annual_payment) if annual_payment > 0 else None

        # --- Spatial autoregression (SAR) for house prices ---
        rho_sar = None
        if n >= 20:
            rho_sar = self._estimate_sar_rho(X, y, n)

        # --- Score ---
        # Bubble risk: 40%, Affordability: 30%, Spatial contagion: 15%, Model fit: 15%
        if prr is not None:
            if prr > 25:
                bubble_score = 90.0
            elif prr > 20:
                bubble_score = 65.0
            elif prr > 16:
                bubble_score = 40.0
            else:
                bubble_score = 15.0
        else:
            bubble_score = 50.0

        if hai is not None:
            if hai < 0.5:
                afford_score = 90.0
            elif hai < 0.8:
                afford_score = 65.0
            elif hai < 1.0:
                afford_score = 45.0
            else:
                afford_score = 15.0
        else:
            afford_score = 50.0

        if rho_sar is not None:
            spatial_score = min(100.0, abs(rho_sar) * 120.0)
        else:
            spatial_score = 50.0

        fit_score = max(0.0, (1.0 - r2) * 100.0)

        score = 0.40 * bubble_score + 0.30 * afford_score + 0.15 * spatial_score + 0.15 * fit_score
        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "n_obs": n,
            "hedonic": {
                "coefficients": dict(zip(coef_names, beta.tolist())),
                "std_errors": dict(zip(coef_names, se.tolist())),
                "r_squared": round(r2, 4),
            },
            "bubble_detection": {
                "price_rent_ratio": round(prr, 2) if prr else None,
                "signal": bubble_signal,
                "n_rent_obs": len(rents),
            },
            "affordability": {
                "hai": round(hai, 3) if hai else None,
                "affordable": hai is not None and hai >= 1.0,
            },
            "spatial_autoregression": {
                "rho": round(rho_sar, 4) if rho_sar else None,
                "strong_spatial_dependence": rho_sar is not None and abs(rho_sar) > 0.5,
            },
        }

    @staticmethod
    def _estimate_sar_rho(X: np.ndarray, y: np.ndarray, n: int) -> float | None:
        """Estimate SAR spatial autocorrelation parameter rho.

        Uses a k-nearest-neighbors spatial weights matrix (row-standardized)
        based on feature similarity (proxy for geographic proximity when
        coordinates are unavailable). Estimates rho via concentrated
        log-likelihood over a grid.
        """
        # Build distance matrix from features (excluding constant)
        features = X[:, 1:]
        # Standardize
        std = features.std(axis=0)
        std[std == 0] = 1.0
        features_std = (features - features.mean(axis=0)) / std

        # k-nearest neighbors (k=min(5, n-1))
        k = min(5, n - 1)
        from scipy.spatial.distance import cdist

        dist_matrix = cdist(features_std, features_std, metric="euclidean")
        W = np.zeros((n, n))
        for i in range(n):
            dists = dist_matrix[i].copy()
            dists[i] = np.inf
            neighbors = np.argsort(dists)[:k]
            W[i, neighbors] = 1.0

        # Row-standardize
        row_sums = W.sum(axis=1)
        row_sums[row_sums == 0] = 1.0
        W = W / row_sums[:, None]

        # Concentrated log-likelihood grid search for rho
        Wy = W @ y
        best_rho = 0.0
        best_ll = -np.inf

        eigenvalues = np.real(la.eigvals(W))

        for rho_candidate in np.linspace(-0.9, 0.9, 37):
            y_star = y - rho_candidate * Wy
            beta_star = np.linalg.lstsq(X, y_star, rcond=None)[0]
            resid_star = y_star - X @ beta_star
            sigma2 = np.sum(resid_star ** 2) / n
            if sigma2 <= 0:
                continue
            # Log-det of (I - rho*W) via eigenvalues
            log_det = float(np.sum(np.log(np.abs(1.0 - rho_candidate * eigenvalues) + 1e-15)))
            ll = -n / 2.0 * np.log(2 * np.pi * sigma2) - n / 2.0 + log_det
            if ll > best_ll:
                best_ll = ll
                best_rho = rho_candidate

        return float(best_rho)
