"""Overconfidence Economics module.

Four dimensions of overconfidence in economic and financial markets:

1. **Calibration curves** (Lichtenstein & Fischhoff 1977):
   Well-calibrated agents assign 70% confidence to events that occur
   70% of the time. Overconfidence = stated confidence > empirical
   hit rate. Measured via forecast surveys and professional prediction
   accuracy (central bank inflation forecasts, analyst earnings forecasts).
   Calibration error: CE = mean(|confidence - accuracy|).

2. **Excess trading costs** (Odean 1999, Barber & Odean 2000):
   Overconfident investors trade too much. Gross returns before trading
   costs may be adequate, but net returns are below market after
   transaction costs. Barber-Odean: men trade 45% more than women and
   earn 2.65 pp/year less. Estimated from turnover rate and alpha decomposition.

3. **Entrepreneurial overoptimism** (Cooper et al. 1988, Camerer & Lovallo 1999):
   93% of US business owners rated their success odds above median.
   Reference class neglect: entrepreneurs ignore base-rate failure rates (~50% by year 5).
   Measured via new business formation vs closure rate, investment
   vs ex-post returns.

4. **Market bubble contribution** (Shiller 2000, DeLong et al. 1990):
   Overconfident noise traders push prices above fundamental value.
   DeLong et al.: noise traders can survive long-run and destabilize prices.
   Estimated via price-earnings deviation from Gordon model value, cyclically
   adjusted PE (CAPE/Shiller PE), and volatility-to-fundamentals ratio.

Score: high calibration error + excess turnover + high firm failure + bubble
indicators -> high stress.

References:
    Lichtenstein, S. & Fischhoff, B. (1977). "Do those who know more also
        know more about how much they know?" OB&HP 20(2).
    Odean, T. (1999). "Do Investors Trade Too Much?" AER 89(5).
    Barber, B. & Odean, T. (2000). "Trading Is Hazardous to Your Wealth."
        JF 55(2).
    Cooper, A., Woo, C. & Dunkelberg, W. (1988). "Entrepreneurs' Perceived
        Chances for Success." Journal of Business Venturing 3(2).
    DeLong, J.B. et al. (1990). "Noise Trader Risk in Financial Markets."
        JPE 98(4).
    Shiller, R. (2000). Irrational Exuberance. Princeton UP.
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class Overconfidence(LayerBase):
    layer_id = "l13"
    name = "Overconfidence"

    async def compute(self, db, **kwargs) -> dict:
        """Estimate overconfidence indicators across financial and real economy.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 code (default USA)
        """
        country = kwargs.get("country_iso3", "USA")

        # Stock market / equity data (for PE ratio, turnover, bubble)
        equity_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name, ds.series_id
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source IN ('fred', 'wdi')
              AND (ds.name LIKE '%price%earnings%ratio%' OR ds.name LIKE '%shiller%cape%'
                   OR ds.name LIKE '%cyclically%adjusted%' OR ds.name LIKE '%stock%market%return%'
                   OR ds.name LIKE '%equity%price%index%' OR ds.name LIKE '%stock%market%capitalization%')
            ORDER BY dp.date
            """,
            (country,),
        )

        # Equity turnover (excess trading proxy)
        turnover_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%stock%turnover%' OR ds.name LIKE '%equity%turnover%'
                   OR ds.name LIKE '%trading%volume%gdp%' OR ds.name LIKE '%shares%traded%')
            ORDER BY dp.date
            """,
            (country,),
        )

        # New business formation / bankruptcy (entrepreneurial overoptimism)
        business_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name, ds.series_id
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%new%business%density%' OR ds.name LIKE '%business%entry%rate%'
                   OR ds.name LIKE '%firm%entry%rate%' OR ds.name LIKE '%startup%formation%'
                   OR ds.name LIKE '%business%closure%rate%')
            ORDER BY dp.date
            """,
            (country,),
        )

        # GDP growth forecast errors (central bank / IMF calibration)
        forecast_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source IN ('imf', 'fred')
              AND (ds.name LIKE '%forecast%error%' OR ds.name LIKE '%inflation%forecast%error%'
                   OR ds.name LIKE '%gdp%forecast%error%' OR ds.name LIKE '%weo%revision%')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not equity_rows and not business_rows and not turnover_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no overconfidence proxy data"}

        # --- 1. Market valuation / bubble indicator (CAPE / P-E) ---
        bubble_analysis = None
        bubble_stress = 0.4
        if equity_rows:
            ev_map: dict[str, list] = {}
            for r in equity_rows:
                ev_map.setdefault(r["series_id"], []).append((r["date"], float(r["value"])))

            # Prefer CAPE-like series
            cape_sid = next(
                (s for s in ev_map if "cape" in s.lower() or "shiller" in s.lower() or "pe" in s.lower()),
                max(ev_map, key=lambda s: len(ev_map[s])),
            )
            primary = sorted(ev_map[cape_sid], key=lambda x: x[0])
            dates = [d for d, _ in primary]
            vals = np.array([v for _, v in primary])
            latest_val = float(vals[-1])

            # Historical CAPE benchmarks: Shiller CAPE < 15 = undervalued, > 25 = elevated, > 35 = extreme
            if np.max(vals) > 10:
                # P/E or CAPE ratio scale
                cape_normalized = float(np.clip((latest_val - 15.0) / 30.0, 0, 1))
                bubble_stress = cape_normalized
            else:
                # Return or other series: use z-score
                mu = float(np.mean(vals))
                sigma = float(np.std(vals, ddof=1)) if len(vals) > 1 else 1.0
                z = (latest_val - mu) / max(sigma, 1e-10)
                bubble_stress = float(np.clip(z / 3.0 + 0.5, 0, 1))

            # Excess volatility test (Shiller 1981)
            excess_vol = None
            if len(vals) >= 10:
                excess_vol = self._excess_volatility_test(vals)

            bubble_analysis = {
                "latest_valuation": round(latest_val, 3),
                "historical_mean": round(float(np.mean(vals)), 3),
                "historical_std": round(float(np.std(vals, ddof=1)) if len(vals) > 1 else 0, 3),
                "bubble_stress": round(bubble_stress, 4),
                "overvalued": latest_val > float(np.mean(vals)) + float(np.std(vals, ddof=1)) if len(vals) > 1 else False,
                "n_obs": len(vals),
                "date_range": [str(dates[0]), str(dates[-1])],
                "reference": "Shiller 2000; DeLong et al. 1990: noise trader bubble contribution",
            }
            if excess_vol:
                bubble_analysis["excess_volatility"] = excess_vol

        # --- 2. Excess trading (Odean-Barber turnover) ---
        turnover_analysis = None
        turnover_stress = 0.4
        if turnover_rows:
            tv = np.array([float(r["value"]) for r in turnover_rows])
            turn_dates = [r["date"] for r in turnover_rows]
            latest_turn = float(tv[-1])

            # World Bank stock turnover ratio: 50-100% = moderate, >200% = high
            if np.max(tv) > 10:
                turn_pct = latest_turn
            else:
                turn_pct = latest_turn * 100.0

            # Higher turnover = more overconfident trading
            turnover_stress = float(np.clip(turn_pct / 200.0, 0, 1))

            turnover_analysis = {
                "latest_turnover_pct": round(turn_pct, 2),
                "mean_turnover_pct": round(float(np.mean(tv)) if np.max(tv) > 10 else float(np.mean(tv)) * 100, 2),
                "excess_trading_stress": round(turnover_stress, 4),
                "odean_threshold": ">100% turnover consistent with excess trading",
                "n_obs": len(tv),
                "date_range": [str(turn_dates[0]), str(turn_dates[-1])],
                "reference": "Odean 1999; Barber & Odean 2000: overconfident investors trade 45% more",
            }

        # --- 3. Entrepreneurial overoptimism ---
        entrepreneurship_analysis = None
        entrepreneur_stress = 0.4
        if business_rows:
            bv_map: dict[str, list] = {}
            for r in business_rows:
                bv_map.setdefault(r["series_id"], []).append((r["date"], float(r["value"])))

            primary_sid = max(bv_map, key=lambda s: len(bv_map[s]))
            primary = sorted(bv_map[primary_sid], key=lambda x: x[0])
            b_dates = [d for d, _ in primary]
            b_vals = np.array([v for _, v in primary])
            latest_b = float(b_vals[-1])

            # New business density (per 1000 workers): high entry with high churn = overoptimism
            # World Bank: typical 3-8 per 1000 workers
            if np.max(b_vals) < 50:
                entry_normalized = float(np.clip(latest_b / 8.0, 0, 1))
            else:
                entry_normalized = float(np.clip(latest_b / 200.0, 0, 1))

            # Paradox: very high entry rates suggest optimism bias
            entrepreneur_stress = entry_normalized

            trend = None
            if len(b_vals) >= 3:
                t = np.arange(len(b_vals), dtype=float)
                slope, _, r_val, p_val, _ = stats.linregress(t, b_vals)
                trend = {
                    "slope": round(float(slope), 4),
                    "direction": "increasing" if slope > 0 else "decreasing",
                    "r_squared": round(float(r_val ** 2), 4),
                }

            entrepreneurship_analysis = {
                "latest_entry_rate": round(latest_b, 3),
                "mean_entry_rate": round(float(np.mean(b_vals)), 3),
                "overoptimism_stress": round(entrepreneur_stress, 4),
                "n_obs": len(b_vals),
                "date_range": [str(b_dates[0]), str(b_dates[-1])],
                "reference": "Cooper et al. 1988: 93% of owners rate success odds above median",
            }
            if trend:
                entrepreneurship_analysis["trend"] = trend

        # --- 4. Forecast calibration error ---
        calibration_analysis = None
        calibration_stress = 0.4
        if forecast_rows:
            fv = np.array([float(r["value"]) for r in forecast_rows])
            fc_dates = [r["date"] for r in forecast_rows]
            latest_fe = float(fv[-1])

            # Forecast errors: mean absolute error as calibration proxy
            mean_abs_error = float(np.mean(np.abs(fv)))
            # WMF: IMF WEO average absolute forecast error ~1-2 pp GDP
            calibration_stress = float(np.clip(mean_abs_error / 3.0, 0, 1))

            calibration_analysis = {
                "latest_forecast_error": round(latest_fe, 4),
                "mean_absolute_error": round(mean_abs_error, 4),
                "calibration_stress": round(calibration_stress, 4),
                "n_obs": len(fv),
                "date_range": [str(fc_dates[0]), str(fc_dates[-1])],
                "reference": "Lichtenstein & Fischhoff 1977: calibration curves; WEO forecast accuracy",
            }

        # --- Score ---
        # Weights: bubble 35, excess trading 25, entrepreneurship 20, calibration 20
        score = float(np.clip(
            bubble_stress * 35.0
            + turnover_stress * 25.0
            + entrepreneur_stress * 20.0
            + calibration_stress * 20.0,
            0, 100,
        ))

        result = {
            "score": round(score, 2),
            "country": country,
            "score_components": {
                "market_bubble_indicator": round(bubble_stress * 35.0, 2),
                "excess_trading": round(turnover_stress * 25.0, 2),
                "entrepreneurial_overoptimism": round(entrepreneur_stress * 20.0, 2),
                "forecast_calibration_error": round(calibration_stress * 20.0, 2),
            },
        }

        if bubble_analysis:
            result["market_valuation_bubble"] = bubble_analysis
        if turnover_analysis:
            result["excess_trading"] = turnover_analysis
        if entrepreneurship_analysis:
            result["entrepreneurial_overoptimism"] = entrepreneurship_analysis
        if calibration_analysis:
            result["forecast_calibration"] = calibration_analysis

        return result

    @staticmethod
    def _excess_volatility_test(vals: np.ndarray) -> dict:
        """Shiller (1981) excess volatility test.

        If stock prices are the present value of future dividends, price
        volatility should not exceed dividend volatility (discounted).
        In practice, prices are far more volatile than fundamentals,
        suggesting speculative overconfidence.

        Uses variance ratio: var(price_changes) / var(dividend proxy).
        Here we use realized price variance vs expected variance under
        random walk with drift as a simplified excess volatility test.
        """
        n = len(vals)
        if n < 10:
            return {"note": "insufficient data"}

        # Observed volatility (log returns)
        log_returns = np.diff(np.log(np.abs(vals) + 1e-10))
        obs_vol = float(np.std(log_returns, ddof=1))

        # Under efficient markets, returns ~ iid with variance = mean(returns)^2 + noise
        # Simple test: compare observed variance to that implied by drift-only model
        mean_return = float(np.mean(log_returns))
        expected_vol = abs(mean_return)  # Under no-excess-volatility

        # Variance ratio
        if expected_vol > 1e-10:
            excess_ratio = obs_vol / expected_vol
        else:
            excess_ratio = obs_vol * 10  # Normalize when drift is near zero

        return {
            "observed_volatility": round(obs_vol, 6),
            "expected_volatility_drift_only": round(expected_vol, 6),
            "excess_volatility_ratio": round(float(np.clip(excess_ratio, 0, 100)), 3),
            "excess_volatile": excess_ratio > 5.0,
            "reference": "Shiller 1981: stock prices more volatile than dividends justify",
            "n_obs": int(len(log_returns)),
        }
