"""Yield Curve Analysis - Term spread, inversion detection, Nelson-Siegel fitting."""

from __future__ import annotations

import numpy as np
from scipy.optimize import minimize

from app.layers.base import LayerBase


class YieldCurve(LayerBase):
    layer_id = "l2"
    name = "Yield Curve"
    weight = 0.05

    # Treasury maturities in months and corresponding FRED series
    MATURITIES = {
        1: "DGS1MO",
        3: "DGS3MO",
        6: "DGS6MO",
        12: "DGS1",
        24: "DGS2",
        36: "DGS3",
        60: "DGS5",
        84: "DGS7",
        120: "DGS10",
        240: "DGS20",
        360: "DGS30",
    }

    # Historical recession lead times for 10Y-2Y inversion (months)
    HISTORICAL_LEADS = [11, 16, 12, 22, 17, 14]  # 1978-2019 inversions

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        lookback = kwargs.get("lookback_years", 10)

        series_ids = list(self.MATURITIES.values())
        rows = await db.execute_fetchall(
            """
            SELECT series_id, date, value FROM data_points
            WHERE series_id IN ({})
              AND country_code = ?
              AND date >= date('now', ?)
            ORDER BY date DESC
            """.format(",".join("?" for _ in series_ids)),
            (*series_ids, country, f"-{lookback} years"),
        )

        # Group by date
        date_map: dict[str, dict[str, float]] = {}
        for r in rows:
            date_map.setdefault(r["date"], {})[r["series_id"]] = float(r["value"])

        if not date_map:
            return {"score": 50.0, "results": {}, "note": "insufficient data"}

        # Latest complete curve (need at least 5 maturities)
        sorted_dates = sorted(date_map.keys(), reverse=True)
        latest_curve = None
        latest_date = None
        for d in sorted_dates:
            if len(date_map[d]) >= 5:
                latest_curve = date_map[d]
                latest_date = d
                break

        if latest_curve is None:
            return {"score": 50.0, "results": {}, "note": "no complete yield curve found"}

        # Build maturity-yield pairs
        mat_months = []
        yields = []
        for m, sid in sorted(self.MATURITIES.items()):
            if sid in latest_curve:
                mat_months.append(m)
                yields.append(latest_curve[sid])

        mat_arr = np.array(mat_months, dtype=float)
        yield_arr = np.array(yields)

        # Term spreads
        spread_10y2y = self._get_spread(latest_curve, "DGS10", "DGS2")
        spread_10y3m = self._get_spread(latest_curve, "DGS10", "DGS3MO")

        # Count inversions across the curve
        n_inversions = 0
        for i in range(len(yields) - 1):
            if yields[i] > yields[i + 1]:
                n_inversions += 1
        inversion_fraction = n_inversions / max(len(yields) - 1, 1)

        # Nelson-Siegel model: y(tau) = beta0 + beta1 * f1(tau) + beta2 * f2(tau)
        # f1(tau) = (1 - exp(-tau/lambda)) / (tau/lambda)
        # f2(tau) = f1(tau) - exp(-tau/lambda)
        ns_params = self._fit_nelson_siegel(mat_arr / 12.0, yield_arr)

        # Build fitted curve
        fitted_yields = self._nelson_siegel_curve(mat_arr / 12.0, ns_params) if ns_params else None

        # Term spread history for recession signal
        spread_history = await self._get_spread_history(db, country, lookback)

        # Recession probability based on term spread
        # Estrella-Mishkin probit: P(recession) = Phi(-0.6045 - 0.7374 * spread)
        recession_prob_3m = None
        recession_prob_12m = None
        if spread_10y3m is not None:
            from scipy.stats import norm
            recession_prob_12m = float(norm.cdf(-0.6045 - 0.7374 * spread_10y3m))
        if spread_10y2y is not None:
            from scipy.stats import norm
            recession_prob_3m = float(norm.cdf(-0.5333 - 0.6330 * spread_10y2y))

        # Slope (basis points per year of maturity)
        if len(mat_arr) >= 2:
            slope_bps = float((yield_arr[-1] - yield_arr[0]) / ((mat_arr[-1] - mat_arr[0]) / 12.0) * 100)
        else:
            slope_bps = 0.0

        # Curvature: 2 * y(5Y) - y(2Y) - y(10Y) (butterfly spread)
        curvature = None
        y2 = latest_curve.get("DGS2")
        y5 = latest_curve.get("DGS5")
        y10 = latest_curve.get("DGS10")
        if y2 is not None and y5 is not None and y10 is not None:
            curvature = 2 * y5 - y2 - y10

        # Score: inverted curve = high stress
        # Weight: 60% inversion indicator, 25% term spread level, 15% curvature
        inversion_score = inversion_fraction * 100.0
        spread_score = 50.0
        if spread_10y2y is not None:
            # Map spread: -1% -> 90, 0% -> 60, +1% -> 30, +2% -> 10
            spread_score = float(np.clip(60.0 - spread_10y2y * 30.0, 0, 100))
        curvature_score = 50.0
        if curvature is not None:
            # Negative curvature = flattening = stress
            curvature_score = float(np.clip(50.0 - curvature * 20.0, 0, 100))

        score = float(np.clip(
            0.60 * inversion_score + 0.25 * spread_score + 0.15 * curvature_score,
            0, 100
        ))

        curve_data = [
            {"maturity_months": int(m), "yield_pct": float(y)}
            for m, y in zip(mat_arr, yield_arr)
        ]

        return {
            "score": score,
            "results": {
                "date": latest_date,
                "spread_10y2y": spread_10y2y,
                "spread_10y3m": spread_10y3m,
                "slope_bps_per_year": slope_bps,
                "curvature": curvature,
                "inversion_fraction": inversion_fraction,
                "n_inversions": n_inversions,
                "recession_prob_3m": recession_prob_3m,
                "recession_prob_12m": recession_prob_12m,
                "nelson_siegel": {
                    "beta0": float(ns_params[0]),
                    "beta1": float(ns_params[1]),
                    "beta2": float(ns_params[2]),
                    "lambda": float(ns_params[3]),
                } if ns_params is not None else None,
                "curve": curve_data,
                "fitted_curve": [
                    {"maturity_months": int(m), "yield_pct": float(y)}
                    for m, y in zip(mat_arr, fitted_yields)
                ] if fitted_yields is not None else None,
                "spread_history": spread_history,
            },
        }

    @staticmethod
    def _get_spread(curve: dict, long_id: str, short_id: str) -> float | None:
        long_val = curve.get(long_id)
        short_val = curve.get(short_id)
        if long_val is not None and short_val is not None:
            return float(long_val - short_val)
        return None

    @staticmethod
    def _nelson_siegel(tau: np.ndarray, beta0: float, beta1: float,
                       beta2: float, lam: float) -> np.ndarray:
        """Nelson-Siegel yield curve model."""
        tau_lam = tau / max(lam, 0.01)
        with np.errstate(divide="ignore", invalid="ignore"):
            f1 = np.where(tau_lam < 1e-6, 1.0, (1 - np.exp(-tau_lam)) / tau_lam)
            f2 = f1 - np.exp(-tau_lam)
        return beta0 + beta1 * f1 + beta2 * f2

    def _fit_nelson_siegel(self, tau_years: np.ndarray,
                           yields: np.ndarray) -> np.ndarray | None:
        """Fit Nelson-Siegel model via nonlinear least squares."""
        if len(tau_years) < 4:
            return None

        def objective(params):
            b0, b1, b2, lam = params
            if lam <= 0.01:
                return 1e10
            fitted = self._nelson_siegel(tau_years, b0, b1, b2, lam)
            return float(np.sum((fitted - yields) ** 2))

        # Initial guess: level, slope, curvature, decay
        y_long = yields[-1]
        y_short = yields[0]
        x0 = [y_long, y_short - y_long, 0.0, 1.5]

        result = minimize(
            objective, x0,
            method="Nelder-Mead",
            options={"maxiter": 2000, "xatol": 1e-6, "fatol": 1e-8},
        )

        if result.success or result.fun < 1.0:
            return result.x
        return None

    def _nelson_siegel_curve(self, tau_years: np.ndarray,
                             params: np.ndarray) -> np.ndarray:
        return self._nelson_siegel(tau_years, params[0], params[1], params[2], params[3])

    async def _get_spread_history(self, db, country: str,
                                  lookback: int) -> list[dict]:
        """Get 10Y-2Y spread history."""
        rows = await db.execute_fetchall(
            """
            SELECT a.date, a.value as y10, b.value as y2
            FROM data_points a
            JOIN data_points b ON a.date = b.date AND b.series_id = 'DGS2' AND b.country_code = ?
            WHERE a.series_id = 'DGS10' AND a.country_code = ?
              AND a.date >= date('now', ?)
            ORDER BY a.date
            """,
            (country, country, f"-{lookback} years"),
        )

        return [
            {"date": r["date"], "spread": float(r["y10"]) - float(r["y2"])}
            for r in rows[-120:]  # Last 120 observations
        ]
