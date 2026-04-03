"""Term structure modeling.

Nelson-Siegel-Svensson yield curve model with 6 parameters for flexible curve
fitting. Vasicek and Cox-Ingersoll-Ross (CIR) short rate models for interest
rate dynamics and bond pricing. Forward rate extraction from fitted curves.

Score (0-100): based on curve fit residuals and implied forward rate anomalies.
Large fitting errors or inverted forward curves push toward STRESS/CRISIS.
"""

from __future__ import annotations

import numpy as np
from scipy.optimize import minimize

from app.layers.base import LayerBase


class TermStructure(LayerBase):
    layer_id = "l7"
    name = "Term Structure"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")
        lookback = kwargs.get("lookback_years", 10)

        rows = await db.fetch_all(
            """
            SELECT ds.description, dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('fred', 'treasury', 'yield_curve')
              AND ds.country_iso3 = ?
              AND dp.date >= date('now', ?)
            ORDER BY dp.date DESC
            """,
            (country, f"-{lookback} years"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no yield data"}

        # Parse yield curve: maturity -> yield
        date_curves: dict[str, dict[float, float]] = {}
        for r in rows:
            desc = (r["description"] or "").lower()
            mat = self._parse_maturity(desc)
            if mat is not None:
                date_curves.setdefault(r["date"], {})[mat] = float(r["value"])

        if not date_curves:
            return {"score": None, "signal": "UNAVAILABLE", "error": "cannot parse maturities"}

        # Use latest curve with sufficient points
        latest_date = None
        latest_curve = None
        for d in sorted(date_curves.keys(), reverse=True):
            if len(date_curves[d]) >= 4:
                latest_date = d
                latest_curve = date_curves[d]
                break

        if latest_curve is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no complete curve"}

        maturities = np.array(sorted(latest_curve.keys()))
        yields = np.array([latest_curve[m] for m in maturities])

        # Nelson-Siegel-Svensson fit
        nss_params = self._fit_nss(maturities, yields)
        nss_fitted = self._nss_curve(maturities, nss_params) if nss_params is not None else None

        # Forward rates from NSS
        forward_rates = None
        if nss_params is not None:
            fine_mats = np.linspace(0.25, max(maturities), 100)
            forward_rates = self._extract_forwards(fine_mats, nss_params)

        # Short rate model calibration from historical data
        # Collect short rate time series
        short_rates = []
        for d in sorted(date_curves.keys()):
            curve = date_curves[d]
            # Use shortest available maturity as proxy
            min_mat = min(curve.keys())
            short_rates.append(curve[min_mat])

        vasicek_params = None
        cir_params = None
        if len(short_rates) >= 24:
            sr = np.array(short_rates)
            dt = 1.0 / 12  # monthly
            vasicek_params = self._calibrate_vasicek(sr, dt)
            cir_params = self._calibrate_cir(sr, dt)

        # Fit quality
        residuals = yields - nss_fitted if nss_fitted is not None else np.zeros_like(yields)
        rmse = float(np.sqrt(np.mean(residuals ** 2)))
        max_abs_error = float(np.max(np.abs(residuals)))

        # Forward rate anomalies (negative forward rates)
        n_negative_forwards = 0
        if forward_rates is not None:
            n_negative_forwards = int(np.sum(np.array([f["rate"] for f in forward_rates]) < 0))

        # Score: poor fit or inverted forwards = stress
        fit_component = float(np.clip(rmse * 200.0, 0, 100))
        forward_component = float(np.clip(
            n_negative_forwards / max(len(forward_rates or [1]), 1) * 100.0, 0, 100))
        # Short rate volatility
        vol_component = 30.0
        if vasicek_params:
            vol_component = float(np.clip(vasicek_params["sigma"] * 500.0, 0, 100))

        score = float(np.clip(
            0.40 * fit_component + 0.35 * forward_component + 0.25 * vol_component,
            0, 100,
        ))

        return {
            "score": round(score, 2),
            "country": country,
            "date": latest_date,
            "n_maturities": len(maturities),
            "observed_curve": [
                {"maturity": round(float(m), 2), "yield_pct": round(float(y), 4)}
                for m, y in zip(maturities, yields)
            ],
            "nelson_siegel_svensson": {
                "beta0": round(float(nss_params[0]), 6),
                "beta1": round(float(nss_params[1]), 6),
                "beta2": round(float(nss_params[2]), 6),
                "beta3": round(float(nss_params[3]), 6),
                "tau1": round(float(nss_params[4]), 4),
                "tau2": round(float(nss_params[5]), 4),
                "rmse": round(rmse, 6),
                "max_error": round(max_abs_error, 6),
            } if nss_params is not None else None,
            "forward_rates": forward_rates[:20] if forward_rates else None,
            "vasicek": vasicek_params,
            "cir": cir_params,
        }

    @staticmethod
    def _parse_maturity(desc: str) -> float | None:
        """Parse maturity in years from description string."""
        import re
        # Match patterns like "3mo", "3 month", "1yr", "10 year", "5y"
        m = re.search(r"(\d+\.?\d*)\s*(mo(?:nth)?s?|yr?(?:ear)?s?)", desc)
        if m:
            val = float(m.group(1))
            unit = m.group(2).lower()
            if unit.startswith("mo"):
                return val / 12.0
            return val
        return None

    @staticmethod
    def _nss_curve(tau: np.ndarray, params: np.ndarray) -> np.ndarray:
        """Nelson-Siegel-Svensson yield curve.

        y(tau) = b0 + b1 * [(1-e^(-tau/t1))/(tau/t1)]
                    + b2 * [(1-e^(-tau/t1))/(tau/t1) - e^(-tau/t1)]
                    + b3 * [(1-e^(-tau/t2))/(tau/t2) - e^(-tau/t2)]
        """
        b0, b1, b2, b3, t1, t2 = params
        t1 = max(t1, 0.01)
        t2 = max(t2, 0.01)

        x1 = tau / t1
        x2 = tau / t2

        with np.errstate(divide="ignore", invalid="ignore"):
            f1 = np.where(x1 < 1e-6, 1.0, (1 - np.exp(-x1)) / x1)
            f2 = f1 - np.exp(-x1)
            g1 = np.where(x2 < 1e-6, 1.0, (1 - np.exp(-x2)) / x2)
            g2 = g1 - np.exp(-x2)

        return b0 + b1 * f1 + b2 * f2 + b3 * g2

    def _fit_nss(self, tau: np.ndarray, yields: np.ndarray) -> np.ndarray | None:
        """Fit Nelson-Siegel-Svensson via nonlinear least squares."""
        if len(tau) < 4:
            return None

        def objective(params):
            fitted = self._nss_curve(tau, params)
            return float(np.sum((fitted - yields) ** 2))

        y_long = yields[-1]
        y_short = yields[0]
        x0 = [y_long, y_short - y_long, 0.0, 0.0, 1.5, 5.0]

        bounds = [
            (-10, 20), (-20, 20), (-20, 20), (-20, 20),
            (0.01, 50), (0.01, 50),
        ]

        result = minimize(
            objective, x0, method="L-BFGS-B", bounds=bounds,
            options={"maxiter": 3000, "ftol": 1e-12},
        )

        if result.fun < len(tau) * 1.0:  # reasonable fit
            return result.x
        return None

    def _extract_forwards(self, maturities: np.ndarray,
                          nss_params: np.ndarray) -> list[dict]:
        """Extract instantaneous forward rates from NSS parameters.

        f(T) = d[T * y(T)] / dT = y(T) + T * dy/dT
        Computed numerically.
        """
        dt = 0.01
        yields_t = self._nss_curve(maturities, nss_params)
        yields_tdt = self._nss_curve(maturities + dt, nss_params)

        # f(T) = [(T+dt)*y(T+dt) - T*y(T)] / dt
        forwards = ((maturities + dt) * yields_tdt - maturities * yields_t) / dt

        return [
            {"maturity": round(float(m), 2), "rate": round(float(f), 4)}
            for m, f in zip(maturities, forwards)
        ]

    @staticmethod
    def _calibrate_vasicek(rates: np.ndarray, dt: float) -> dict:
        """Calibrate Vasicek model: dr = kappa*(theta - r)*dt + sigma*dW.

        OLS on: r(t+1) - r(t) = a + b*r(t) + eps
        => kappa = -b/dt, theta = -a/(b), sigma = std(eps)/sqrt(dt)
        """
        dr = np.diff(rates)
        r_lag = rates[:-1]
        n = len(dr)

        X = np.column_stack([np.ones(n), r_lag])
        beta = np.linalg.lstsq(X, dr, rcond=None)[0]
        residuals = dr - X @ beta

        a, b = float(beta[0]), float(beta[1])
        sigma_eps = float(np.std(residuals, ddof=2))

        kappa = -b / dt
        theta = -a / b if abs(b) > 1e-10 else float(np.mean(rates))
        sigma = sigma_eps / np.sqrt(dt)

        # Feller condition not required for Vasicek (can go negative)
        return {
            "kappa": round(kappa, 4),
            "theta": round(theta, 4),
            "sigma": round(sigma, 4),
            "long_run_mean": round(theta, 4),
            "half_life_years": round(np.log(2) / max(kappa, 0.01), 2),
        }

    @staticmethod
    def _calibrate_cir(rates: np.ndarray, dt: float) -> dict:
        """Calibrate CIR model: dr = kappa*(theta - r)*dt + sigma*sqrt(r)*dW.

        OLS on: dr/sqrt(r) = a/sqrt(r) + b*sqrt(r) + eps
        => kappa = -b/dt, theta = -a/(b), sigma = std(eps)/sqrt(dt)
        """
        rates_pos = np.maximum(rates, 1e-6)  # CIR requires positive rates
        dr = np.diff(rates_pos)
        r_lag = rates_pos[:-1]
        sqrt_r = np.sqrt(r_lag)

        # Transformed regression
        y = dr / sqrt_r
        X = np.column_stack([1.0 / sqrt_r, sqrt_r])
        beta = np.linalg.lstsq(X, y, rcond=None)[0]
        residuals = y - X @ beta

        a, b = float(beta[0]), float(beta[1])
        sigma_eps = float(np.std(residuals, ddof=2))

        kappa = -b / dt
        theta = -a / b if abs(b) > 1e-10 else float(np.mean(rates_pos))
        sigma = sigma_eps / np.sqrt(dt)

        # Feller condition: 2*kappa*theta >= sigma^2
        feller_satisfied = 2 * kappa * theta >= sigma ** 2

        return {
            "kappa": round(kappa, 4),
            "theta": round(theta, 4),
            "sigma": round(sigma, 4),
            "feller_condition": feller_satisfied,
            "long_run_mean": round(theta, 4),
            "half_life_years": round(np.log(2) / max(kappa, 0.01), 2),
        }
