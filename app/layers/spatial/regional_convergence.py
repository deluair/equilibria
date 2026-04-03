"""Regional convergence with spatial econometrics.

Extends classical beta convergence to account for spatial dependence across
regions. Poor regions may grow faster (convergence), but spatial spillovers
from neighbors can accelerate or retard this process.

Spatial beta convergence models:
    SAR: g_r = rho * W * g_r + beta * ln(y_{r,0}) + X * gamma + e
    SEM: g_r = beta * ln(y_{r,0}) + X * gamma + u,  u = lambda * W * u + e

where W is a row-standardized spatial weights matrix, rho captures spatial
lag in growth, and lambda captures spatially correlated errors.

Moran's I for spatial autocorrelation (Moran 1950):
    I = (n / S0) * (e' W e) / (e' e)
    where S0 = sum of all weights.
    Under H0 (no spatial autocorrelation), z-score tests significance.

LISA (Local Indicators of Spatial Association, Anselin 1995):
    I_i = z_i * sum_j(w_ij * z_j)
    Identifies spatial clusters (High-High, Low-Low) and outliers (High-Low,
    Low-High).

Spatial clubs (Durlauf & Johnson 1995, extended):
    Regions may converge to different steady states (club convergence) rather
    than a single equilibrium. Identified via regression tree on initial
    conditions.

References:
    Anselin, L. (1995). Local Indicators of Spatial Association. Geographical
        Analysis 27(2): 93-115.
    Rey, S. & Montouri, B. (1999). US Regional Income Convergence: A Spatial
        Econometric Perspective. Regional Studies 33(2): 143-156.
    Durlauf, S. & Johnson, P. (1995). Multiple Regimes and Cross-Country
        Growth Behaviour. Journal of Applied Econometrics 10(4).

Score: divergence + significant positive spatial autocorrelation in residuals
(spatial inequality clustering) -> STRESS.
"""

import json

import numpy as np
from scipy import stats
from scipy.spatial.distance import cdist

from app.layers.base import LayerBase


class RegionalConvergence(LayerBase):
    layer_id = "l11"
    name = "Regional Convergence"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata, ds.description
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'regional_gdp'
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows or len(rows) < 10:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient regional data"}

        # Parse regional panel: region -> [(year, gdppc, lat, lon)]
        regions: dict[str, list] = {}
        for row in rows:
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            region = meta.get("region")
            year = meta.get("year")
            gdppc = row["value"]
            if region is None or year is None or gdppc is None or gdppc <= 0:
                continue
            lat = meta.get("lat", 0.0)
            lon = meta.get("lon", 0.0)
            regions.setdefault(region, []).append((int(year), float(gdppc), float(lat), float(lon)))

        if len(regions) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient regions"}

        # Compute initial income and average growth per region
        initial_log_y = []
        avg_growth = []
        coords = []
        region_names = []

        for reg, series in regions.items():
            series.sort(key=lambda x: x[0])
            vals = [v for _, v, _, _ in series if v > 0]
            if len(vals) < 3:
                continue
            y0 = vals[0]
            yt = vals[-1]
            t = len(vals) - 1
            if t <= 0:
                continue
            g = (np.log(yt) - np.log(y0)) / t
            initial_log_y.append(np.log(y0))
            avg_growth.append(g)
            coords.append([series[0][2], series[0][3]])
            region_names.append(reg)

        n = len(initial_log_y)
        if n < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient valid regions"}

        y = np.array(avg_growth)
        x = np.array(initial_log_y)
        coords_arr = np.array(coords)

        # --- Build spatial weights matrix ---
        W = self._build_spatial_weights(coords_arr, n)

        # --- Unconditional beta convergence (OLS) ---
        X_ols = np.column_stack([np.ones(n), x])
        beta_ols = np.linalg.lstsq(X_ols, y, rcond=None)[0]
        resid_ols = y - X_ols @ beta_ols
        ss_res = np.sum(resid_ols ** 2)
        ss_tot = np.sum((y - y.mean()) ** 2)
        r2_ols = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

        beta_conv = float(beta_ols[1])
        convergence = beta_conv < 0
        speed = -beta_conv if convergence else 0.0
        half_life = np.log(2) / speed if speed > 0 else float("inf")

        # --- Moran's I on OLS residuals ---
        morans_i, morans_z, morans_p = self._morans_i(resid_ols, W, n)

        # --- LISA clusters ---
        lisa_clusters = self._lisa(resid_ols, W, n, region_names)

        # --- SAR estimation via concentrated ML ---
        sar_rho = self._sar_rho(X_ols, y, W, n)

        # --- Spatial clubs detection ---
        clubs = self._detect_clubs(x, y, n, region_names)

        # --- Score ---
        # Divergence: 40%, Spatial autocorrelation: 30%, Club fragmentation: 30%
        if convergence and beta_conv < -0.01:
            conv_score = 15.0
        elif convergence:
            conv_score = 35.0
        elif beta_conv < 0.01:
            conv_score = 50.0
        else:
            conv_score = min(90.0, 50.0 + beta_conv * 2000.0)

        # Strong positive Moran's I = spatial clustering of inequality
        if morans_p < 0.05 and morans_i > 0:
            spatial_score = min(90.0, 40.0 + morans_i * 100.0)
        elif morans_p < 0.10 and morans_i > 0:
            spatial_score = 40.0
        else:
            spatial_score = 20.0

        club_score = min(80.0, len(clubs) * 20.0) if clubs else 30.0

        score = 0.40 * conv_score + 0.30 * spatial_score + 0.30 * club_score
        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "n_regions": n,
            "beta_convergence": {
                "beta": round(beta_conv, 6),
                "convergence": convergence,
                "speed": round(speed, 6),
                "half_life": round(half_life, 2) if half_life != float("inf") else None,
                "r_squared": round(r2_ols, 4),
            },
            "morans_i": {
                "I": round(morans_i, 4),
                "z_score": round(morans_z, 4),
                "p_value": round(morans_p, 6),
                "significant": morans_p < 0.05,
            },
            "sar": {
                "rho": round(sar_rho, 4) if sar_rho is not None else None,
            },
            "lisa_clusters": lisa_clusters[:10],  # Top 10
            "spatial_clubs": clubs,
        }

    @staticmethod
    def _build_spatial_weights(coords: np.ndarray, n: int) -> np.ndarray:
        """Build row-standardized spatial weights from coordinates.

        Uses k-nearest neighbors if coordinates have variation, otherwise
        falls back to equal weights.
        """
        if np.std(coords) < 1e-10:
            # No spatial variation, use equal weights
            W = np.ones((n, n)) / (n - 1)
            np.fill_diagonal(W, 0.0)
            return W

        dist_matrix = cdist(coords, coords, metric="euclidean")
        k = min(5, n - 1)
        W = np.zeros((n, n))
        for i in range(n):
            dists = dist_matrix[i].copy()
            dists[i] = np.inf
            neighbors = np.argsort(dists)[:k]
            W[i, neighbors] = 1.0

        row_sums = W.sum(axis=1)
        row_sums[row_sums == 0] = 1.0
        W = W / row_sums[:, None]
        return W

    @staticmethod
    def _morans_i(residuals: np.ndarray, W: np.ndarray, n: int) -> tuple[float, float, float]:
        """Global Moran's I with inference."""
        z = residuals - residuals.mean()
        S0 = W.sum()
        if S0 == 0 or np.sum(z ** 2) == 0:
            return 0.0, 0.0, 1.0

        moran_stat = (n / S0) * float(z @ W @ z) / float(z @ z)

        # Expected value under randomization
        E_I = -1.0 / (n - 1)  # noqa: N806

        # Variance under randomization (simplified)
        S1 = float(np.sum((W + W.T) ** 2)) / 2.0
        S2 = float(np.sum((W.sum(axis=1) + W.sum(axis=0)) ** 2))
        b2 = n * np.sum(z ** 4) / (np.sum(z ** 2) ** 2)

        A = n * ((n ** 2 - 3 * n + 3) * S1 - n * S2 + 3 * S0 ** 2)  # noqa: N806
        B = b2 * ((n ** 2 - n) * S1 - 2 * n * S2 + 6 * S0 ** 2)  # noqa: N806
        C = (n - 1) * (n - 2) * (n - 3) * S0 ** 2  # noqa: N806

        if C == 0:
            return moran_stat, 0.0, 1.0

        var_I = (A - B) / C - E_I ** 2  # noqa: N806
        var_I = max(var_I, 1e-15)  # noqa: N806
        z_score = (moran_stat - E_I) / np.sqrt(var_I)
        p_value = 2.0 * (1.0 - stats.norm.cdf(abs(z_score)))

        return float(moran_stat), float(z_score), float(p_value)

    @staticmethod
    def _lisa(
        residuals: np.ndarray, W: np.ndarray, n: int, names: list[str]
    ) -> list[dict]:
        """Local Indicators of Spatial Association."""
        z = residuals - residuals.mean()
        std_z = np.std(z)
        if std_z == 0:
            return []
        z_std = z / std_z

        lisa_values = []
        for i in range(n):
            Wz = float(W[i] @ z_std)
            li = float(z_std[i]) * Wz

            # Classify: HH, LL, HL, LH
            if z_std[i] > 0 and Wz > 0:
                cluster = "High-High"
            elif z_std[i] < 0 and Wz < 0:
                cluster = "Low-Low"
            elif z_std[i] > 0 and Wz < 0:
                cluster = "High-Low"
            elif z_std[i] < 0 and Wz > 0:
                cluster = "Low-High"
            else:
                cluster = "Not Significant"

            lisa_values.append({
                "region": names[i],
                "lisa_i": round(li, 4),
                "cluster": cluster,
            })

        # Sort by absolute LISA value descending
        lisa_values.sort(key=lambda x: -abs(x["lisa_i"]))
        return lisa_values

    @staticmethod
    def _sar_rho(X: np.ndarray, y: np.ndarray, W: np.ndarray, n: int) -> float | None:
        """Estimate SAR rho via concentrated log-likelihood grid search."""
        from scipy import linalg as la

        Wy = W @ y
        eigenvalues = np.real(la.eigvals(W))
        best_rho = 0.0
        best_ll = -np.inf

        for rho in np.linspace(-0.9, 0.9, 37):
            y_star = y - rho * Wy
            beta_star = np.linalg.lstsq(X, y_star, rcond=None)[0]
            resid_star = y_star - X @ beta_star
            sigma2 = np.sum(resid_star ** 2) / n
            if sigma2 <= 0:
                continue
            log_det = float(np.sum(np.log(np.abs(1.0 - rho * eigenvalues) + 1e-15)))
            ll = -n / 2.0 * np.log(2 * np.pi * sigma2) - n / 2.0 + log_det
            if ll > best_ll:
                best_ll = ll
                best_rho = rho

        return float(best_rho)

    @staticmethod
    def _detect_clubs(
        initial_log_y: np.ndarray, growth: np.ndarray, n: int, names: list[str]
    ) -> list[dict]:
        """Detect convergence clubs via median split on initial income.

        Tests whether sub-groups converge internally (separate beta < 0).
        """
        if n < 10:
            return []

        median_y = np.median(initial_log_y)
        low_mask = initial_log_y <= median_y
        high_mask = ~low_mask

        clubs = []
        for label, mask in [("low_income", low_mask), ("high_income", high_mask)]:
            if mask.sum() < 4:
                continue
            x_club = initial_log_y[mask]
            y_club = growth[mask]
            X_club = np.column_stack([np.ones(mask.sum()), x_club])
            beta_club = np.linalg.lstsq(X_club, y_club, rcond=None)[0]
            clubs.append({
                "club": label,
                "n_regions": int(mask.sum()),
                "beta": round(float(beta_club[1]), 6),
                "converging": float(beta_club[1]) < 0,
                "members": [names[i] for i in range(n) if mask[i]][:5],
            })

        return clubs
