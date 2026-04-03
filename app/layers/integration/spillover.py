"""Spillover Detection (Diebold-Yilmaz Framework).

Measures shock transmission across analytical layers using forecast error
variance decomposition from a VAR model. Identifies which layers are net
transmitters vs. net receivers of shocks.
"""

import json
import logging
from datetime import datetime, timezone

import numpy as np
from scipy import linalg

from app.layers.base import LayerBase

logger = logging.getLogger(__name__)

LAYER_IDS = ["l1", "l2", "l3", "l4", "l5"]
LAYER_NAMES = {
    "l1": "Trade",
    "l2": "Macro",
    "l3": "Labor",
    "l4": "Development",
    "l5": "Agricultural",
}

# VAR lag order
DEFAULT_VAR_LAGS = 2

# Forecast horizon for variance decomposition
DEFAULT_HORIZON = 10

# Minimum time series length
MIN_OBS = 20


class SpilloverDetection(LayerBase):
    layer_id = "l6"
    name = "Spillover Detection"

    async def compute(self, db, **kwargs) -> dict:
        country_iso3 = kwargs.get("country_iso3", "USA")
        var_lags = kwargs.get("var_lags", DEFAULT_VAR_LAGS)
        horizon = kwargs.get("horizon", DEFAULT_HORIZON)
        lookback = kwargs.get("lookback", 104)  # ~2 years weekly

        # Fetch layer score time series
        layer_series = await self._fetch_layer_series(
            db, country_iso3, lookback
        )

        available = [lid for lid in LAYER_IDS if lid in layer_series]
        if len(available) < 2:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "spillover_index": None,
                "country_iso3": country_iso3,
                "reason": f"Need at least 2 layer series, got {len(available)}",
            }

        # Align and difference the series (stationarity)
        data = self._prepare_data(layer_series, available)

        if data.shape[0] < MIN_OBS:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "spillover_index": None,
                "country_iso3": country_iso3,
                "reason": f"Insufficient observations ({data.shape[0]} < {MIN_OBS})",
            }

        # Estimate VAR
        var_coeffs, residuals, sigma = self._estimate_var(data, var_lags)

        if var_coeffs is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "spillover_index": None,
                "country_iso3": country_iso3,
                "reason": "VAR estimation failed (singular matrix)",
            }

        # Generalized forecast error variance decomposition
        fevd = self._compute_gfevd(var_coeffs, sigma, horizon, len(available))

        # Diebold-Yilmaz spillover table
        spillover_table = self._compute_spillover_table(fevd, available)

        # Total spillover index
        total_spillover = self._total_spillover_index(fevd)

        # Directional spillovers
        directional = self._directional_spillovers(fevd, available)

        # Net spillovers
        net = self._net_spillovers(directional, available)

        # Score: total spillover (0-100 scale, already percentage)
        score = min(total_spillover, 100.0)

        await self._store_result(
            db, country_iso3, score, total_spillover,
            spillover_table, directional, net,
        )

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "spillover_index": round(total_spillover, 2),
            "spillover_table": spillover_table,
            "directional": directional,
            "net_spillovers": net,
            "var_lags": var_lags,
            "forecast_horizon": horizon,
            "layers_analyzed": available,
            "observations": data.shape[0],
            "country_iso3": country_iso3,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "methodology": "Diebold-Yilmaz (2012) generalized FEVD spillover index",
        }

    async def _fetch_layer_series(
        self, db, country_iso3: str, lookback: int
    ) -> dict[str, list[float]]:
        series = {}
        for lid in LAYER_IDS:
            rows = await db.fetch_all(
                """
                SELECT score FROM analysis_results
                WHERE layer = ? AND country_iso3 = ? AND score IS NOT NULL
                ORDER BY created_at DESC LIMIT ?
                """,
                (lid, country_iso3, lookback),
            )
            if rows:
                series[lid] = [r["score"] for r in reversed(rows)]
        return series

    def _prepare_data(
        self, layer_series: dict[str, list[float]], available: list[str]
    ) -> np.ndarray:
        """Align series and first-difference for stationarity."""
        min_len = min(len(layer_series[lid]) for lid in available)
        raw = np.zeros((min_len, len(available)))
        for j, lid in enumerate(available):
            s = layer_series[lid]
            raw[:, j] = s[len(s) - min_len:]

        # First difference
        if min_len > 1:
            return np.diff(raw, axis=0)
        return raw

    def _estimate_var(
        self, data: np.ndarray, lags: int
    ) -> tuple[np.ndarray | None, np.ndarray | None, np.ndarray | None]:
        """Estimate VAR(p) via OLS.

        Returns coefficient matrices (k*p x k), residuals, and error covariance.
        """
        t, k = data.shape
        if t <= k * lags + 1:
            return None, None, None

        # Build lagged matrix
        y = data[lags:]  # (T-p) x k
        x_parts = []
        for lag in range(1, lags + 1):
            x_parts.append(data[lags - lag: t - lag])
        # Add constant
        x = np.column_stack(x_parts + [np.ones(t - lags)])  # (T-p) x (k*p + 1)

        try:
            # OLS: B = (X'X)^-1 X'Y
            xtx = x.T @ x
            xty = x.T @ y
            coeffs = linalg.solve(xtx, xty, assume_a="sym")
        except linalg.LinAlgError:
            return None, None, None

        residuals = y - x @ coeffs
        sigma = (residuals.T @ residuals) / (t - lags - k * lags - 1)

        # Return only the VAR coefficients (exclude constant)
        var_coeffs = coeffs[:-1]  # (k*p) x k
        return var_coeffs, residuals, sigma

    def _compute_gfevd(
        self, var_coeffs: np.ndarray, sigma: np.ndarray,
        horizon: int, k: int,
    ) -> np.ndarray:
        """Generalized Forecast Error Variance Decomposition (Pesaran-Shin).

        Returns k x k matrix where element (i,j) is the fraction of i's
        forecast error variance attributable to shocks in j.
        """
        lags = var_coeffs.shape[0] // k

        # Compute MA representation (impulse response matrices)
        # Phi_s for s = 0, 1, ..., horizon
        phi = [np.eye(k)]  # Phi_0 = I
        for s in range(1, horizon + 1):
            phi_s = np.zeros((k, k))
            for lag in range(1, min(s, lags) + 1):
                a_lag = var_coeffs[(lag - 1) * k: lag * k].T  # k x k
                phi_s += a_lag @ phi[s - lag]
            phi.append(phi_s)

        # Generalized FEVD
        sigma_diag = np.diag(sigma)
        theta = np.zeros((k, k))

        for i in range(k):
            for j in range(k):
                numerator = 0.0
                denominator = 0.0
                for s in range(horizon + 1):
                    e_j = np.zeros(k)
                    e_j[j] = 1.0
                    psi_s_sigma_ej = phi[s] @ sigma @ e_j
                    numerator += psi_s_sigma_ej[i] ** 2

                    # Total variance of variable i at horizon s
                    for h in range(k):
                        e_h = np.zeros(k)
                        e_h[h] = 1.0
                        psi_s_sigma_eh = phi[s] @ sigma @ e_h
                        denominator += psi_s_sigma_eh[i] ** 2

                # Scale by inverse of sigma_jj
                if sigma_diag[j] > 1e-10 and denominator > 1e-10:
                    theta[i, j] = (numerator / sigma_diag[j]) / (
                        denominator / sigma_diag[j]
                    )
                else:
                    theta[i, j] = 0.0

        # Normalize rows to sum to 1
        row_sums = theta.sum(axis=1, keepdims=True)
        row_sums = np.where(row_sums > 1e-10, row_sums, 1.0)
        theta = theta / row_sums

        return theta

    def _compute_spillover_table(
        self, fevd: np.ndarray, labels: list[str]
    ) -> dict:
        """Human-readable spillover table."""
        k = len(labels)
        table = {}
        for i in range(k):
            row = {}
            for j in range(k):
                row[labels[j]] = round(float(fevd[i, j] * 100), 2)
            row["from_others"] = round(
                float(sum(fevd[i, j] for j in range(k) if j != i) * 100), 2
            )
            table[labels[i]] = row

        # Column sums (contribution to others)
        to_others = {}
        for j in range(k):
            to_others[labels[j]] = round(
                float(sum(fevd[i, j] for i in range(k) if i != j) * 100), 2
            )
        table["to_others"] = to_others

        return table

    def _total_spillover_index(self, fevd: np.ndarray) -> float:
        """Total spillover: sum of off-diagonal / k * 100."""
        k = fevd.shape[0]
        off_diag_sum = float(np.sum(fevd) - np.trace(fevd))
        return off_diag_sum / k * 100.0

    def _directional_spillovers(
        self, fevd: np.ndarray, labels: list[str]
    ) -> dict:
        """Directional spillovers: FROM others and TO others for each layer."""
        k = len(labels)
        result = {}
        for i in range(k):
            from_others = float(
                sum(fevd[i, j] for j in range(k) if j != i) * 100
            )
            to_others = float(
                sum(fevd[j, i] for j in range(k) if j != i) * 100
            )
            result[labels[i]] = {
                "name": LAYER_NAMES.get(labels[i], labels[i]),
                "from_others": round(from_others, 2),
                "to_others": round(to_others, 2),
                "net": round(to_others - from_others, 2),
                "role": "transmitter" if to_others > from_others else "receiver",
            }
        return result

    def _net_spillovers(
        self, directional: dict, labels: list[str]
    ) -> list[dict]:
        """Sorted list of net spillovers (positive = net transmitter)."""
        nets = []
        for lid in labels:
            d = directional[lid]
            nets.append({
                "layer": lid,
                "name": d["name"],
                "net_spillover": d["net"],
                "role": d["role"],
            })
        return sorted(nets, key=lambda x: x["net_spillover"], reverse=True)

    async def _store_result(
        self, db, country_iso3: str, score: float,
        total_spillover: float, table: dict, directional: dict, net: list,
    ):
        await db.execute(
            """
            INSERT INTO analysis_results (analysis_type, country_iso3, layer, parameters, result, score, signal)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "spillover_detection",
                country_iso3,
                "l6",
                json.dumps({"method": "diebold_yilmaz_gfevd"}),
                json.dumps({
                    "total_spillover": round(total_spillover, 2),
                    "net_spillovers": net,
                }),
                round(score, 2),
                self.classify_signal(score),
            ),
        )
