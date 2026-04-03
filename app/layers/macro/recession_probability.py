"""Recession Probability - Probit model using term spread, credit conditions, labor market."""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats
from scipy.optimize import minimize

from app.layers.base import LayerBase


class RecessionProbability(LayerBase):
    layer_id = "l2"
    name = "Recession Probability"
    weight = 0.05

    # Predictor series
    PREDICTORS = {
        "term_spread_10y3m": ("DGS10", "DGS3MO"),   # 10Y-3M Treasury spread
        "term_spread_10y2y": ("DGS10", "DGS2"),      # 10Y-2Y Treasury spread
        "credit_spread": "BAMLC0A0CM",               # Corporate credit spread
        "vix": "VIXCLS",                              # Equity volatility
        "initial_claims": "ICSA",                     # Initial unemployment claims
        "leading_index": "USSLIND",                   # Leading Economic Index
        "ism_pmi": "MANEMP",                          # ISM Manufacturing Employment
        "consumer_sentiment": "UMCSENT",              # U Michigan Consumer Sentiment
    }

    # NBER recession dates indicator
    RECESSION_SERIES = "USREC"  # NBER Recession Indicator

    # Estrella-Mishkin (1998) probit coefficients (pre-estimated)
    # P(recession in 12 months) = Phi(alpha + beta * spread)
    EM_ALPHA = -0.6045
    EM_BETA = -0.7374

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        lookback = kwargs.get("lookback_years", 40)
        forecast_horizons = kwargs.get("horizons", [3, 6, 12])

        # Gather all series
        all_series = [self.RECESSION_SERIES]
        spread_series = []
        for key, val in self.PREDICTORS.items():
            if isinstance(val, tuple):
                spread_series.extend(val)
            else:
                all_series.append(val)
        all_series.extend(spread_series)

        rows = await db.execute_fetchall(
            """
            SELECT series_id, date, value FROM data_points
            WHERE series_id IN ({})
              AND country_code = ?
              AND date >= date('now', ?)
            ORDER BY series_id, date
            """.format(",".join("?" for _ in all_series)),
            (*all_series, country, f"-{lookback} years"),
        )

        series_map: dict[str, dict[str, float]] = {}
        for r in rows:
            series_map.setdefault(r["series_id"], {})[r["date"]] = float(r["value"])

        results = {}

        # Compute spread predictors
        predictor_values = {}
        for name, spec in self.PREDICTORS.items():
            if isinstance(spec, tuple):
                long_id, short_id = spec
                long_data = series_map.get(long_id, {})
                short_data = series_map.get(short_id, {})
                common = sorted(set(long_data.keys()) & set(short_data.keys()))
                if common:
                    predictor_values[name] = {
                        d: long_data[d] - short_data[d] for d in common
                    }
            else:
                if spec in series_map:
                    predictor_values[name] = series_map[spec]

        # Method 1: Estrella-Mishkin probit (term spread only)
        em_probs = {}
        spread_data = predictor_values.get("term_spread_10y3m", {})
        if spread_data:
            latest_date = sorted(spread_data.keys())[-1]
            latest_spread = spread_data[latest_date]
            em_probs["12m_ahead"] = float(
                sp_stats.norm.cdf(self.EM_ALPHA + self.EM_BETA * latest_spread)
            )
            # 3-month ahead: different coefficients (steeper slope)
            em_probs["3m_ahead"] = float(
                sp_stats.norm.cdf(-0.5333 - 0.6330 * latest_spread)
            )
            em_probs["current_spread"] = latest_spread
            em_probs["date"] = latest_date

            # Historical recession probabilities
            spread_dates = sorted(spread_data.keys())
            em_probs["history"] = [
                {
                    "date": d,
                    "spread": spread_data[d],
                    "prob_12m": float(sp_stats.norm.cdf(
                        self.EM_ALPHA + self.EM_BETA * spread_data[d]
                    )),
                }
                for d in spread_dates[-120:]
            ]

        results["estrella_mishkin"] = em_probs

        # Method 2: Multi-predictor probit (if recession data available)
        recession_data = series_map.get(self.RECESSION_SERIES, {})
        if recession_data and len(predictor_values) >= 3:
            probit_result = self._fit_multivariate_probit(
                recession_data, predictor_values, forecast_horizons
            )
            results["multivariate_probit"] = probit_result

        # Method 3: Composite leading indicator approach
        composite = self._composite_indicator(predictor_values, series_map)
        results["composite_indicator"] = composite

        # Method 4: Sahm Rule (real-time recession indicator)
        sahm = await self._sahm_rule(db, country)
        results["sahm_rule"] = sahm

        # Current recession probabilities summary
        probs = {}
        for horizon in forecast_horizons:
            key = f"{horizon}m_ahead"
            if key in em_probs:
                probs[key] = em_probs[key]
            elif "multivariate_probit" in results:
                mv = results["multivariate_probit"]
                if key in mv:
                    probs[key] = mv[key]

        results["summary_probabilities"] = probs

        # Score: higher recession probability = higher stress
        max_prob = max(probs.values()) if probs else 0.3
        score = float(np.clip(max_prob * 100, 0, 100))

        return {
            "score": score,
            "results": results,
        }

    def _fit_multivariate_probit(self, recession_data: dict,
                                 predictor_values: dict,
                                 horizons: list[int]) -> dict:
        """Fit probit model with multiple predictors at different horizons."""
        # Align all series to common dates
        all_dates = set(recession_data.keys())
        for pv in predictor_values.values():
            all_dates &= set(pv.keys())
        common = sorted(all_dates)

        if len(common) < 30:
            return {"note": "insufficient data for multivariate probit"}

        # Build predictor matrix
        pred_names = list(predictor_values.keys())
        X = np.column_stack([
            [predictor_values[name][d] for d in common]
            for name in pred_names
        ])

        results = {}
        for horizon in horizons:
            if horizon >= len(common):
                continue

            # Dependent variable: recession indicator h months ahead
            y = np.array([
                recession_data.get(common[min(i + horizon, len(common) - 1)], 0)
                for i in range(len(common))
            ])

            # Standardize predictors
            X_std = (X - np.mean(X, axis=0)) / np.maximum(np.std(X, axis=0, ddof=1), 1e-12)

            # Fit probit via MLE
            coeffs = self._probit_mle(y, X_std)
            if coeffs is not None:
                # Current probability
                x_latest = X_std[-1]
                linear_pred = coeffs[0] + x_latest @ coeffs[1:]
                prob = float(sp_stats.norm.cdf(linear_pred))

                # Marginal effects (at mean)
                phi_at_mean = float(sp_stats.norm.pdf(coeffs[0]))
                marginal_effects = {
                    name: float(coeffs[i + 1] * phi_at_mean)
                    for i, name in enumerate(pred_names)
                }

                results[f"{horizon}m_ahead"] = prob
                results[f"{horizon}m_coefficients"] = {
                    "intercept": float(coeffs[0]),
                    **{name: float(coeffs[i + 1]) for i, name in enumerate(pred_names)},
                }
                results[f"{horizon}m_marginal_effects"] = marginal_effects

        return results

    @staticmethod
    def _probit_mle(y: np.ndarray, X: np.ndarray) -> np.ndarray | None:
        """Maximum likelihood estimation of probit model."""
        n, k = X.shape

        def neg_log_likelihood(params):
            alpha = params[0]
            beta = params[1:]
            z = alpha + X @ beta
            # Clip to avoid numerical issues
            z = np.clip(z, -10, 10)
            p = sp_stats.norm.cdf(z)
            p = np.clip(p, 1e-10, 1 - 1e-10)
            ll = np.sum(y * np.log(p) + (1 - y) * np.log(1 - p))
            return -ll

        x0 = np.zeros(k + 1)
        result = minimize(neg_log_likelihood, x0, method="BFGS",
                          options={"maxiter": 1000})

        if result.success or result.fun < n * 10:
            return result.x
        return None

    def _composite_indicator(self, predictor_values: dict,
                             series_map: dict) -> dict:
        """Build composite recession indicator from standardized predictors."""
        # Weights for each predictor (based on empirical forecasting power)
        predictor_weights = {
            "term_spread_10y3m": -0.30,   # negative: inversion predicts recession
            "term_spread_10y2y": -0.15,
            "credit_spread": 0.20,        # positive: wider spreads = stress
            "vix": 0.10,
            "initial_claims": 0.10,
            "leading_index": -0.10,       # negative: lower LEI = recession risk
            "consumer_sentiment": -0.05,
        }

        latest_z_scores = {}
        composite = 0.0
        total_weight = 0.0

        for name, weight in predictor_weights.items():
            data = predictor_values.get(name, {})
            if not data:
                continue

            dates = sorted(data.keys())
            vals = np.array([data[d] for d in dates])

            if len(vals) < 12:
                continue

            mean = float(np.mean(vals))
            std = float(np.std(vals, ddof=1))
            if std < 1e-12:
                continue

            z = (vals[-1] - mean) / std
            latest_z_scores[name] = float(z)
            composite += weight * z
            total_weight += abs(weight)

        if total_weight > 0:
            composite /= total_weight

        # Convert to probability-like scale using normal CDF
        prob = float(sp_stats.norm.cdf(composite))

        return {
            "composite_z": float(composite),
            "implied_probability": prob,
            "predictor_z_scores": latest_z_scores,
            "signal": (
                "high_risk" if prob > 0.7 else
                "elevated" if prob > 0.4 else
                "moderate" if prob > 0.2 else
                "low_risk"
            ),
        }

    async def _sahm_rule(self, db, country: str) -> dict:
        """Sahm Rule: recession signaled when 3-month moving average of unemployment
        rises 0.5 pp above its low from the prior 12 months."""
        rows = await db.execute_fetchall(
            """
            SELECT date, value FROM data_points
            WHERE series_id = 'UNRATE' AND country_code = ?
              AND date >= date('now', '-5 years')
            ORDER BY date
            """,
            (country,),
        )

        if len(rows) < 15:
            return {"triggered": False, "note": "insufficient data"}

        dates = [r["date"] for r in rows]
        vals = np.array([float(r["value"]) for r in rows])

        # 3-month moving average
        if len(vals) < 3:
            return {"triggered": False, "note": "insufficient data"}

        ma3 = np.convolve(vals, np.ones(3) / 3, mode="valid")
        ma3_dates = dates[2:]

        # For each month, find the minimum of the prior 12 months' MA
        sahm_values = []
        for i in range(12, len(ma3)):
            min_prior_12 = float(np.min(ma3[max(0, i - 12):i]))
            sahm_val = float(ma3[i] - min_prior_12)
            sahm_values.append({"date": ma3_dates[i], "value": sahm_val})

        current_sahm = sahm_values[-1]["value"] if sahm_values else 0.0

        return {
            "triggered": current_sahm >= 0.5,
            "current_value": current_sahm,
            "threshold": 0.5,
            "series": sahm_values[-36:],  # Last 3 years
        }
