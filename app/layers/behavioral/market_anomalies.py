"""Market Anomalies module.

Identifies behavioral finance anomalies that violate the Efficient Market Hypothesis.

1. **Calendar Effects** (Thaler 1987):
   - January effect: abnormal returns in January (small-cap premium).
   - Monday effect: negative returns on Mondays (weekend information processing).
   - Turn-of-month effect: positive returns in last/first days of month.
   Tested via dummy variable regressions with HAC standard errors.

2. **Momentum and Reversal** (Jegadeesh & Titman 1993, De Bondt & Thaler 1985):
   - Momentum: past winners outperform past losers over 3-12 months.
   - Long-run reversal: past losers outperform past winners over 3-5 years.
   Estimated via autocorrelation at multiple horizons and cross-sectional
   return predictability.

3. **Post-Earnings Announcement Drift** (Ball & Brown 1968, Bernard & Thomas 1989):
   Prices continue to drift in the direction of earnings surprises
   for 60-90 days post-announcement. Measured via autocorrelation of
   abnormal returns following large moves (earnings proxy).

4. **Limits to Arbitrage** (Shleifer & Vishny 1997):
   Mispricings persist when arbitrage is costly or risky.
   Measured via noise trader risk, fundamental risk, and
   implementation costs proxied by volatility clustering and
   bid-ask spread analogs.

Score reflects anomaly intensity: strong, persistent anomalies -> higher
stress (market inefficiency risk). Efficient markets -> low score.

Sources: FRED (market returns, volatility), WDI (stock market data)
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


def _newey_west_se(X: np.ndarray, resid: np.ndarray, max_lag: int = 4) -> np.ndarray:
    """Newey-West HAC standard errors for OLS coefficients."""
    n, k = X.shape
    bread = np.linalg.inv(X.T @ X)

    # Meat: S_0 + sum of weighted cross-products
    S = X.T @ np.diag(resid**2) @ X
    for lag in range(1, max_lag + 1):
        weight = 1 - lag / (max_lag + 1)
        gamma = X[lag:].T @ np.diag(resid[lag:] * resid[:-lag]) @ X[:-lag]
        S += weight * (gamma + gamma.T)

    vcov = bread @ S @ bread
    return np.sqrt(np.maximum(np.diag(vcov), 0))


class MarketAnomalies(LayerBase):
    layer_id = "l13"
    name = "Market Anomalies"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        # Fetch return/price data
        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND (ds.series_id LIKE '%SP500%' OR ds.series_id LIKE '%STOCK%'
                   OR ds.series_id LIKE '%MARKET%' OR ds.series_id LIKE '%INDEX%'
                   OR ds.series_id IN ('CM.MKT.TRAD.GD.ZS', 'CM.MKT.LCAP.GD.ZS'))
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 24:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient market data"}

        values = np.array([float(r["value"]) for r in rows])
        dates = [r["date"] for r in rows]

        # Compute returns (log returns for price data, or use directly if growth rates)
        if np.mean(values) > 10:
            # Likely price levels, compute log returns
            values_pos = np.maximum(values, 1e-10)
            returns = np.diff(np.log(values_pos))
        else:
            # Already growth rates
            returns = values[1:] / 100 if np.mean(np.abs(values)) > 1 else values[1:]

        if len(returns) < 20:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient return data"}

        results = {
            "country": country,
            "n_obs": len(returns),
            "period": f"{dates[0]} to {dates[-1]}",
        }

        # --- 1. Calendar Effects ---
        calendar = self._calendar_effects(returns, dates[1:])
        results["calendar_effects"] = calendar

        # --- 2. Momentum and Reversal ---
        momentum = self._momentum_reversal(returns)
        results["momentum_reversal"] = momentum

        # --- 3. Post-Announcement Drift ---
        drift = self._post_announcement_drift(returns)
        results["post_announcement_drift"] = drift

        # --- 4. Limits to Arbitrage ---
        limits = self._limits_to_arbitrage(returns)
        results["limits_to_arbitrage"] = limits

        # --- Score ---
        # Count significant anomalies
        anomaly_count = 0
        anomaly_strength = 0

        if calendar.get("any_significant"):
            anomaly_count += 1
            anomaly_strength += calendar.get("max_t_stat", 0) * 5

        if momentum.get("momentum_significant"):
            anomaly_count += 1
            anomaly_strength += abs(momentum.get("momentum_autocorr", 0)) * 30

        if momentum.get("reversal_significant"):
            anomaly_count += 1
            anomaly_strength += abs(momentum.get("reversal_autocorr", 0)) * 30

        if drift.get("drift_significant"):
            anomaly_count += 1
            anomaly_strength += drift.get("drift_ratio", 0) * 20

        if limits.get("arbitrage_constrained"):
            anomaly_count += 1
            anomaly_strength += limits.get("noise_trader_risk", 0) * 20

        # Base score from anomaly count
        base = anomaly_count * 15
        # Add strength
        score = min(100, base + anomaly_strength)

        return {"score": round(score, 1), **results}

    @staticmethod
    def _calendar_effects(returns: np.ndarray, dates: list[str]) -> dict:
        """Test for calendar anomalies via dummy regression.

        January effect, Monday effect (if daily), turn-of-month.
        Uses Newey-West HAC standard errors.
        """
        n = len(returns)

        # Extract month from dates (works with YYYY-MM-DD or YYYY-MM formats)
        months = []
        for d in dates:
            parts = str(d).split("-")
            if len(parts) >= 2:
                try:
                    months.append(int(parts[1]))
                except (ValueError, IndexError):
                    months.append(0)
            else:
                months.append(0)

        months = np.array(months)

        result = {
            "any_significant": False,
            "max_t_stat": 0,
        }

        # January effect
        if np.sum(months == 1) >= 2 and np.sum(months != 1) >= 2:
            jan_dummy = (months == 1).astype(float)
            X = np.column_stack([np.ones(n), jan_dummy])
            beta = np.linalg.lstsq(X, returns, rcond=None)[0]
            resid = returns - X @ beta
            se = _newey_west_se(X, resid, max_lag=min(4, n // 5))

            jan_t = float(beta[1] / se[1]) if se[1] > 1e-10 else 0
            jan_p = float(2 * (1 - sp_stats.t.cdf(abs(jan_t), df=max(n - 2, 1))))

            result["january_effect"] = {
                "coefficient": round(float(beta[1]), 6),
                "t_stat": round(jan_t, 4),
                "p_value": round(jan_p, 6),
                "significant": jan_p < 0.05,
                "mean_jan_return": round(float(np.mean(returns[months == 1])), 6),
                "mean_other_return": round(float(np.mean(returns[months != 1])), 6),
            }

            if jan_p < 0.05:
                result["any_significant"] = True
            result["max_t_stat"] = max(result["max_t_stat"], abs(jan_t))

        # Turn-of-month effect (last 2 and first 2 observations of each group)
        # Approximate: test first and last observations in each monthly group
        # Group consecutive obs by month
        month_change = np.where(np.diff(months) != 0)[0]
        if len(month_change) >= 3:
            turn_of_month = np.zeros(n)
            for idx in month_change:
                # Mark 2 before and 2 after month change
                for offset in [-1, 0, 1, 2]:
                    pos = idx + offset
                    if 0 <= pos < n:
                        turn_of_month[pos] = 1

            if np.sum(turn_of_month) >= 2 and np.sum(turn_of_month == 0) >= 2:
                X_tom = np.column_stack([np.ones(n), turn_of_month])
                beta_tom = np.linalg.lstsq(X_tom, returns, rcond=None)[0]
                resid_tom = returns - X_tom @ beta_tom
                se_tom = _newey_west_se(X_tom, resid_tom, max_lag=min(4, n // 5))

                tom_t = float(beta_tom[1] / se_tom[1]) if se_tom[1] > 1e-10 else 0
                tom_p = float(2 * (1 - sp_stats.t.cdf(abs(tom_t), df=max(n - 2, 1))))

                result["turn_of_month"] = {
                    "coefficient": round(float(beta_tom[1]), 6),
                    "t_stat": round(tom_t, 4),
                    "p_value": round(tom_p, 6),
                    "significant": tom_p < 0.05,
                }

                if tom_p < 0.05:
                    result["any_significant"] = True
                result["max_t_stat"] = max(result["max_t_stat"], abs(tom_t))

        # Sell-in-May (May-October vs November-April)
        summer = np.isin(months, [5, 6, 7, 8, 9, 10]).astype(float)
        if np.sum(summer) >= 2 and np.sum(summer == 0) >= 2:
            X_sim = np.column_stack([np.ones(n), summer])
            beta_sim = np.linalg.lstsq(X_sim, returns, rcond=None)[0]
            resid_sim = returns - X_sim @ beta_sim
            se_sim = _newey_west_se(X_sim, resid_sim, max_lag=min(4, n // 5))

            sim_t = float(beta_sim[1] / se_sim[1]) if se_sim[1] > 1e-10 else 0
            sim_p = float(2 * (1 - sp_stats.t.cdf(abs(sim_t), df=max(n - 2, 1))))

            result["sell_in_may"] = {
                "coefficient": round(float(beta_sim[1]), 6),
                "t_stat": round(sim_t, 4),
                "p_value": round(sim_p, 6),
                "significant": sim_p < 0.05,
            }

            if sim_p < 0.05:
                result["any_significant"] = True
            result["max_t_stat"] = max(result["max_t_stat"], abs(sim_t))

        result["max_t_stat"] = round(result["max_t_stat"], 4)
        return result

    @staticmethod
    def _momentum_reversal(returns: np.ndarray) -> dict:
        """Test for momentum (short-horizon) and reversal (long-horizon).

        Jegadeesh & Titman (1993): momentum at 3-12 month horizon.
        De Bondt & Thaler (1985): reversal at 36-60 month horizon.

        Estimated via autocorrelation at multiple lags.
        """
        n = len(returns)
        result = {
            "momentum_significant": False,
            "reversal_significant": False,
        }

        # Compute autocorrelations at various horizons
        autocorrelations = {}
        test_lags = [1, 3, 6, 12, 24, 36]
        for lag in test_lags:
            if n - lag < 5:
                continue
            corr = float(np.corrcoef(returns[:-lag], returns[lag:])[0, 1])
            if not np.isfinite(corr):
                continue
            autocorrelations[lag] = corr

            # Test significance: t = r * sqrt(n-2) / sqrt(1-r^2)
            r = corr
            se_r = 1 / np.sqrt(n - lag - 2) if n - lag > 2 else 1
            t_stat = r / se_r
            p_val = float(2 * (1 - sp_stats.t.cdf(abs(t_stat), df=max(n - lag - 2, 1))))

            autocorrelations[f"{lag}_t"] = round(t_stat, 4)
            autocorrelations[f"{lag}_p"] = round(p_val, 6)

        # Momentum: positive autocorrelation at short horizons (1-12 months)
        short_lags = [lag for lag in [1, 3, 6, 12] if lag in autocorrelations]
        if short_lags:
            mom_lag = max(short_lags, key=lambda lag: autocorrelations.get(lag, 0))
            mom_corr = autocorrelations.get(mom_lag, 0)
            mom_p = autocorrelations.get(f"{mom_lag}_p", 1)
            result["momentum_autocorr"] = round(mom_corr, 4)
            result["momentum_lag"] = mom_lag
            result["momentum_p"] = round(mom_p, 6)
            result["momentum_significant"] = mom_p < 0.05 and mom_corr > 0

        # Reversal: negative autocorrelation at long horizons (24-36 months)
        long_lags = [lag for lag in [24, 36] if lag in autocorrelations]
        if long_lags:
            rev_lag = min(long_lags, key=lambda lag: autocorrelations.get(lag, 0))
            rev_corr = autocorrelations.get(rev_lag, 0)
            rev_p = autocorrelations.get(f"{rev_lag}_p", 1)
            result["reversal_autocorr"] = round(rev_corr, 4)
            result["reversal_lag"] = rev_lag
            result["reversal_p"] = round(rev_p, 6)
            result["reversal_significant"] = rev_p < 0.05 and rev_corr < 0

        result["autocorrelation_profile"] = {
            k: round(v, 4) for k, v in autocorrelations.items() if isinstance(k, int)
        }

        return result

    @staticmethod
    def _post_announcement_drift(returns: np.ndarray) -> dict:
        """Test for post-announcement drift (Ball & Brown 1968).

        Large returns (proxy for earnings surprises) should be followed
        by continued drift if PEAD exists. Measured by conditioning
        on large |return| events and tracking cumulative abnormal
        returns in the following periods.
        """
        n = len(returns)
        if n < 20:
            return {"drift_significant": False, "note": "insufficient data"}

        # Identify "announcement" events: returns in top/bottom 10%
        abs_returns = np.abs(returns)
        threshold = np.percentile(abs_returns, 90)
        event_idx = np.where(abs_returns >= threshold)[0]

        if len(event_idx) < 3:
            return {"drift_significant": False, "note": "too few events"}

        # Track cumulative returns after events
        windows = [1, 3, 6, 12]
        drift_by_window = {}

        for window in windows:
            post_returns = []
            for idx in event_idx:
                if idx + window < n:
                    # Cumulative return after event
                    cum_ret = float(np.sum(returns[idx + 1 : idx + 1 + window]))
                    # Sign matches event direction?
                    sign_match = np.sign(returns[idx]) == np.sign(cum_ret)
                    post_returns.append((cum_ret, sign_match, float(returns[idx])))

            if len(post_returns) < 3:
                continue

            cum_rets = np.array([pr[0] for pr in post_returns])
            event_signs = np.array([pr[2] for pr in post_returns])

            # Correlation between event return and post-event drift
            corr = float(np.corrcoef(event_signs, cum_rets)[0, 1])
            if not np.isfinite(corr):
                corr = 0

            # Proportion of same-sign continuation
            sign_matches = sum(1 for pr in post_returns if pr[1]) / len(post_returns)

            drift_by_window[window] = {
                "mean_drift": round(float(np.mean(cum_rets)), 6),
                "drift_event_corr": round(corr, 4),
                "sign_continuation_rate": round(sign_matches, 4),
                "n_events": len(post_returns),
            }

        # Overall drift assessment
        drift_significant = False
        drift_ratio = 0
        if drift_by_window:
            best_window = max(
                drift_by_window,
                key=lambda w: abs(drift_by_window[w]["drift_event_corr"]),
            )
            best = drift_by_window[best_window]
            drift_ratio = abs(best["drift_event_corr"])

            # Significant if correlation > 0.2 and sign continuation > 60%
            if drift_ratio > 0.2 and best["sign_continuation_rate"] > 0.6:
                drift_significant = True

        return {
            "drift_significant": drift_significant,
            "drift_ratio": round(drift_ratio, 4),
            "drift_by_window": drift_by_window,
            "n_events": len(event_idx),
            "event_threshold": round(float(threshold), 6),
            "reference": "Ball & Brown 1968, Bernard & Thomas 1989",
        }

    @staticmethod
    def _limits_to_arbitrage(returns: np.ndarray) -> dict:
        """Measure limits to arbitrage (Shleifer & Vishny 1997).

        Arbitrage is limited when:
        1. Noise trader risk is high (volatility clustering)
        2. Fundamental risk is high (return dispersion)
        3. Implementation costs are high (illiquidity proxy)
        """
        n = len(returns)
        if n < 10:
            return {"arbitrage_constrained": False, "note": "insufficient data"}

        # Noise trader risk: GARCH-like volatility clustering
        # Measured by autocorrelation of squared returns
        sq_returns = returns**2
        if n >= 5:
            vol_clustering = float(np.corrcoef(sq_returns[:-1], sq_returns[1:])[0, 1])
            vol_clustering = max(0, vol_clustering) if np.isfinite(vol_clustering) else 0
        else:
            vol_clustering = 0

        # Fundamental risk: tail thickness (kurtosis)
        kurt = float(sp_stats.kurtosis(returns, fisher=True))

        # Implementation cost proxy: serial correlation of absolute returns
        # (high = predictable volatility = easier arbitrage)
        abs_ret = np.abs(returns)
        if n >= 5:
            abs_autocorr = float(np.corrcoef(abs_ret[:-1], abs_ret[1:])[0, 1])
            abs_autocorr = abs_autocorr if np.isfinite(abs_autocorr) else 0
        else:
            abs_autocorr = 0

        # Noise trader risk index
        noise_risk = min(1.0, max(0, vol_clustering * 2))

        # Fundamental risk index
        fund_risk = min(1.0, max(0, kurt / 10))

        # Implementation cost (inverse of predictability)
        impl_cost = min(1.0, max(0, 1 - abs_autocorr))

        # Overall arbitrage constraint index
        arb_constraint = (noise_risk + fund_risk + impl_cost) / 3
        arbitrage_constrained = arb_constraint > 0.5

        # Variance ratio test (Lo & MacKinlay 1988)
        # VR(q) = Var(r_t(q)) / (q * Var(r_t)) should equal 1 under RW
        variance_ratios = {}
        for q in [2, 4, 8]:
            if n >= q * 3:
                # q-period returns
                q_returns = np.array([
                    np.sum(returns[i : i + q]) for i in range(0, n - q + 1, q)
                ])
                if len(q_returns) >= 3:
                    var_q = float(np.var(q_returns, ddof=1))
                    var_1 = float(np.var(returns, ddof=1))
                    vr = var_q / (q * var_1) if var_1 > 0 else 1.0
                    variance_ratios[q] = round(vr, 4)

        return {
            "arbitrage_constrained": arbitrage_constrained,
            "noise_trader_risk": round(noise_risk, 4),
            "fundamental_risk": round(fund_risk, 4),
            "implementation_cost": round(impl_cost, 4),
            "constraint_index": round(arb_constraint, 4),
            "volatility_clustering": round(vol_clustering, 4),
            "excess_kurtosis": round(kurt, 4),
            "variance_ratios": variance_ratios,
            "reference": "Shleifer & Vishny 1997: limits to arbitrage",
        }
