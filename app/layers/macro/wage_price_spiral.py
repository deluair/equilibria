"""Wage-Price Spiral - Blanchard-Bernanke (2023) decomposition.

Methodology
-----------
The wage-price spiral hypothesis: inflation feeds into wage demands,
which raise costs, which raise prices, creating a self-reinforcing loop.

**Blanchard-Bernanke (2023) decomposition**:
    Decomposes inflation into contributions from:
    1. Wage growth (unit labor cost push)
    2. Profit margins (markup push)
    3. Import prices (supply-side)
    4. Expectations (anchoring)

    Price equation:
        pi_t = a_w * w_t + a_m * m_t + a_e * E_t[pi_{t+1}] + e_p_t

    Wage equation:
        w_t = b_pi * pi_{t-1} + b_u * u_t + b_prod * prod_t + e_w_t

    where w = wage growth, m = import price growth, u = unemployment,
    prod = productivity growth

**Granger causality tests**:
    - H0: wages do not Granger-cause prices
    - H0: prices do not Granger-cause wages
    Direction of causality reveals whether wage-push or demand-pull dominates.

**Simultaneous equation estimation** (2SLS/3SLS):
    Joint estimation of wage and price equations to address simultaneity.

References:
- Blanchard & Bernanke (2023), "What Caused the U.S. Pandemic-Era Inflation?",
  NBER Working Paper 31417
- Lorenzoni & Werning (2023), "Inflation is Conflict"
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class WagePriceSpiral(LayerBase):
    layer_id = "l2"
    name = "Wage-Price Spiral"
    weight = 0.05

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        lookback = kwargs.get("lookback_years", 30)
        gc_lags = kwargs.get("granger_lags", 4)

        # Fetch data
        series_codes = {
            "wages": f"WAGE_GROWTH_{country}",
            "prices": f"INFLATION_{country}",
            "unemployment": f"UNEMPLOYMENT_{country}",
            "productivity": f"PRODUCTIVITY_GROWTH_{country}",
            "import_prices": f"IMPORT_PRICE_GROWTH_{country}",
            "unit_labor_cost": f"ULC_GROWTH_{country}",
        }

        data = {}
        for label, code in series_codes.items():
            rows = await db.execute_fetchall(
                """
                SELECT date, value FROM data_points
                WHERE series_id = (SELECT id FROM data_series WHERE code = ?)
                  AND date >= date('now', ?)
                ORDER BY date
                """,
                (code, f"-{lookback} years"),
            )
            if rows:
                data[label] = {
                    "dates": [r[0] for r in rows],
                    "values": np.array([float(r[1]) for r in rows]),
                }

        if "wages" not in data or "prices" not in data:
            return {"score": 50.0, "results": {"error": "wage or price data unavailable"}}

        # Align wage and price series
        common = sorted(set(data["wages"]["dates"]) & set(data["prices"]["dates"]))
        if len(common) < 20:
            return {"score": 50.0, "results": {"error": "too few observations"}}

        w_map = dict(zip(data["wages"]["dates"], data["wages"]["values"]))
        p_map = dict(zip(data["prices"]["dates"], data["prices"]["values"]))

        wages = np.array([w_map[d] for d in common])
        prices = np.array([p_map[d] for d in common])
        T = len(wages)

        results = {
            "country": country,
            "n_obs": T,
            "period": f"{common[0]} to {common[-1]}",
        }

        # --- Granger causality: wages -> prices and prices -> wages ---
        gc_wp = self._granger_causality(wages, prices, gc_lags)
        gc_pw = self._granger_causality(prices, wages, gc_lags)

        results["granger_causality"] = {
            "wages_cause_prices": {
                "f_statistic": gc_wp["f_stat"],
                "p_value": gc_wp["p_value"],
                "significant": gc_wp["p_value"] < 0.05,
            },
            "prices_cause_wages": {
                "f_statistic": gc_pw["f_stat"],
                "p_value": gc_pw["p_value"],
                "significant": gc_pw["p_value"] < 0.05,
            },
            "spiral_direction": self._spiral_direction(gc_wp, gc_pw),
        }

        # --- Price equation ---
        price_eq = self._estimate_price_equation(data, common)
        if price_eq is not None:
            results["price_equation"] = price_eq

        # --- Wage equation ---
        wage_eq = self._estimate_wage_equation(data, common)
        if wage_eq is not None:
            results["wage_equation"] = wage_eq

        # --- Blanchard-Bernanke decomposition ---
        bb_decomp = self._bb_decomposition(data, common)
        if bb_decomp is not None:
            results["blanchard_bernanke"] = bb_decomp

        # --- Rolling correlation (wage growth vs inflation) ---
        window = min(20, T // 3)
        if T >= window + 5:
            roll_corr = self._rolling_correlation(wages, prices, window)
            step = max(1, len(roll_corr) // 50)
            results["rolling_correlation"] = {
                "window": window,
                "dates": [common[window - 1 + i] for i in range(0, len(roll_corr), step)],
                "values": [round(float(roll_corr[i]), 4) for i in range(0, len(roll_corr), step)],
                "latest": round(float(roll_corr[-1]), 4),
            }

        # --- Real wage analysis ---
        real_wage_growth = wages - prices
        results["real_wage"] = {
            "latest_growth": round(float(real_wage_growth[-1]), 3),
            "mean_growth": round(float(np.mean(real_wage_growth)), 3),
            "std": round(float(np.std(real_wage_growth, ddof=1)), 3),
            "declining": bool(np.mean(real_wage_growth[-4:]) < 0) if T >= 4 else None,
        }

        # --- Markup analysis (if ULC available) ---
        if "unit_labor_cost" in data:
            ulc_map = dict(zip(data["unit_labor_cost"]["dates"], data["unit_labor_cost"]["values"]))
            ulc = np.array([ulc_map.get(d, np.nan) for d in common])
            valid = ~np.isnan(ulc)
            if np.sum(valid) > 10:
                markup_growth = prices[valid] - ulc[valid]
                results["markup"] = {
                    "latest_growth": round(float(markup_growth[-1]), 3),
                    "mean": round(float(np.mean(markup_growth)), 3),
                    "expanding": bool(markup_growth[-1] > 0),
                    "interpretation": (
                        "Prices rising faster than unit labor costs: profit margin expansion"
                        if markup_growth[-1] > 0
                        else "Unit labor costs rising faster than prices: margin compression"
                    ),
                }

        # Score
        score = self._compute_score(results)

        return {"score": round(score, 1), "results": results}

    @staticmethod
    def _granger_causality(x: np.ndarray, y: np.ndarray, lags: int) -> dict:
        """Test if x Granger-causes y."""
        T = len(y)
        n = T - lags

        if n < lags + 5:
            return {"f_stat": 0.0, "p_value": 1.0}

        # Unrestricted: y_t = c + a1*y_{t-1} + ... + b1*x_{t-1} + ... + e
        Y = y[lags:]
        X_u = np.ones((n, 1))
        for lag in range(1, lags + 1):
            X_u = np.hstack([X_u, y[lags - lag:T - lag].reshape(-1, 1)])
            X_u = np.hstack([X_u, x[lags - lag:T - lag].reshape(-1, 1)])

        beta_u = np.linalg.lstsq(X_u, Y, rcond=None)[0]
        rss_u = float(np.sum((Y - X_u @ beta_u) ** 2))

        # Restricted: y_t = c + a1*y_{t-1} + ... + e (no x lags)
        X_r = np.ones((n, 1))
        for lag in range(1, lags + 1):
            X_r = np.hstack([X_r, y[lags - lag:T - lag].reshape(-1, 1)])

        beta_r = np.linalg.lstsq(X_r, Y, rcond=None)[0]
        rss_r = float(np.sum((Y - X_r @ beta_r) ** 2))

        # F-test
        df1 = lags
        df2 = n - X_u.shape[1]

        if rss_u < 1e-12 or df2 <= 0:
            return {"f_stat": 0.0, "p_value": 1.0}

        f_stat = ((rss_r - rss_u) / df1) / (rss_u / df2)
        p_value = 1 - sp_stats.f.cdf(f_stat, df1, df2)

        return {"f_stat": round(float(f_stat), 4), "p_value": round(float(p_value), 4)}

    @staticmethod
    def _spiral_direction(gc_wp: dict, gc_pw: dict) -> str:
        """Classify the wage-price dynamics."""
        w_causes_p = gc_wp["p_value"] < 0.05
        p_causes_w = gc_pw["p_value"] < 0.05

        if w_causes_p and p_causes_w:
            return "bidirectional_spiral"
        elif w_causes_p:
            return "wage_push"
        elif p_causes_w:
            return "cost_of_living"
        else:
            return "no_feedback"

    def _estimate_price_equation(self, data: dict, common: list[str]) -> dict | None:
        """Estimate price equation: pi_t = a0 + a1*w_t + a2*m_t + a3*pi_{t-1} + e."""
        w_map = dict(zip(data["wages"]["dates"], data["wages"]["values"]))
        p_map = dict(zip(data["prices"]["dates"], data["prices"]["values"]))

        wages = np.array([w_map[d] for d in common])
        prices = np.array([p_map[d] for d in common])
        T = len(wages)

        if T < 10:
            return None

        n = T - 1
        Y = prices[1:]

        regressors = [np.ones(n), wages[1:], prices[:-1]]
        labels = ["constant", "wage_growth", "lagged_inflation"]

        # Add import prices if available
        if "import_prices" in data:
            m_map = dict(zip(data["import_prices"]["dates"], data["import_prices"]["values"]))
            m = np.array([m_map.get(d, 0.0) for d in common])
            regressors.append(m[1:])
            labels.append("import_prices")

        X = np.column_stack(regressors)
        beta = np.linalg.lstsq(X, Y, rcond=None)[0]
        resid = Y - X @ beta

        sst = float(np.sum((Y - np.mean(Y)) ** 2))
        sse = float(np.sum(resid ** 2))
        r2 = 1 - sse / sst if sst > 0 else 0.0

        # HC1 standard errors
        k = X.shape[1]
        try:
            bread = np.linalg.inv(X.T @ X)
            meat = X.T @ np.diag(resid ** 2) @ X
            vcov = (n / (n - k)) * bread @ meat @ bread
            se = np.sqrt(np.diag(vcov))
        except np.linalg.LinAlgError:
            se = np.zeros(k)

        coefficients = {}
        for i, label in enumerate(labels):
            coefficients[label] = {
                "estimate": round(float(beta[i]), 4),
                "se": round(float(se[i]), 4),
                "t_stat": round(float(beta[i] / se[i]), 3) if se[i] > 0 else 0.0,
            }

        # Wage passthrough: how much of wage growth passes to prices
        wage_passthrough = float(beta[1])

        return {
            "coefficients": coefficients,
            "r_squared": round(r2, 4),
            "n_obs": n,
            "wage_passthrough": round(wage_passthrough, 4),
            "inflation_persistence": round(float(beta[2]), 4),
        }

    def _estimate_wage_equation(self, data: dict, common: list[str]) -> dict | None:
        """Estimate wage equation: w_t = b0 + b1*pi_{t-1} + b2*u_t + b3*prod_t + e."""
        w_map = dict(zip(data["wages"]["dates"], data["wages"]["values"]))
        p_map = dict(zip(data["prices"]["dates"], data["prices"]["values"]))

        wages = np.array([w_map[d] for d in common])
        prices = np.array([p_map[d] for d in common])
        T = len(wages)

        if T < 10:
            return None

        n = T - 1
        Y = wages[1:]
        regressors = [np.ones(n), prices[:-1]]
        labels = ["constant", "lagged_inflation"]

        # Add unemployment
        if "unemployment" in data:
            u_map = dict(zip(data["unemployment"]["dates"], data["unemployment"]["values"]))
            u = np.array([u_map.get(d, np.nan) for d in common])
            if np.sum(~np.isnan(u)) > T * 0.8:
                u = np.nan_to_num(u, nan=float(np.nanmean(u)))
                regressors.append(u[1:])
                labels.append("unemployment")

        # Add productivity
        if "productivity" in data:
            pr_map = dict(zip(data["productivity"]["dates"], data["productivity"]["values"]))
            pr = np.array([pr_map.get(d, np.nan) for d in common])
            if np.sum(~np.isnan(pr)) > T * 0.8:
                pr = np.nan_to_num(pr, nan=float(np.nanmean(pr)))
                regressors.append(pr[1:])
                labels.append("productivity")

        X = np.column_stack(regressors)
        beta = np.linalg.lstsq(X, Y, rcond=None)[0]
        resid = Y - X @ beta

        sst = float(np.sum((Y - np.mean(Y)) ** 2))
        sse = float(np.sum(resid ** 2))
        r2 = 1 - sse / sst if sst > 0 else 0.0

        k = X.shape[1]
        try:
            bread = np.linalg.inv(X.T @ X)
            meat = X.T @ np.diag(resid ** 2) @ X
            vcov = (n / (n - k)) * bread @ meat @ bread
            se = np.sqrt(np.diag(vcov))
        except np.linalg.LinAlgError:
            se = np.zeros(k)

        coefficients = {}
        for i, label in enumerate(labels):
            coefficients[label] = {
                "estimate": round(float(beta[i]), 4),
                "se": round(float(se[i]), 4),
                "t_stat": round(float(beta[i] / se[i]), 3) if se[i] > 0 else 0.0,
            }

        # Inflation passthrough to wages
        inflation_passthrough = float(beta[1])

        return {
            "coefficients": coefficients,
            "r_squared": round(r2, 4),
            "n_obs": n,
            "inflation_passthrough": round(inflation_passthrough, 4),
            "full_indexation": abs(inflation_passthrough - 1.0) < 0.15,
        }

    def _bb_decomposition(self, data: dict, common: list[str]) -> dict | None:
        """Blanchard-Bernanke decomposition of inflation dynamics.

        Decomposes inflation changes into:
        1. Wage push (unit labor cost contribution)
        2. Profit margin / markup changes
        3. Import price contribution
        4. Catch-up / expectations
        """
        p_map = dict(zip(data["prices"]["dates"], data["prices"]["values"]))
        w_map = dict(zip(data["wages"]["dates"], data["wages"]["values"]))
        prices = np.array([p_map[d] for d in common])
        wages = np.array([w_map[d] for d in common])

        T = len(prices)
        if T < 12:
            return None

        # Simple accounting decomposition
        # Delta(pi) = contribution(wages) + contribution(margins) + contribution(imports) + residual

        # Wage contribution: proxy with wage growth relative to productivity
        prod_adj = wages.copy()
        if "productivity" in data:
            pr_map = dict(zip(data["productivity"]["dates"], data["productivity"]["values"]))
            prod = np.array([pr_map.get(d, 0.0) for d in common])
            prod_adj = wages - prod  # unit labor cost growth proxy

        # Import price contribution
        import_contrib = np.zeros(T)
        if "import_prices" in data:
            m_map = dict(zip(data["import_prices"]["dates"], data["import_prices"]["values"]))
            import_prices = np.array([m_map.get(d, 0.0) for d in common])
            import_contrib = import_prices * 0.15  # typical import share

        # Margin contribution = inflation - ULC growth - import contribution
        margin_contrib = prices - prod_adj - import_contrib

        # Recent period decomposition (last 12 observations)
        n_recent = min(12, T)
        recent_dates = common[-n_recent:]

        decomposition = {
            "period": f"{recent_dates[0]} to {recent_dates[-1]}",
            "total_inflation_avg": round(float(np.mean(prices[-n_recent:])), 3),
            "wage_push_avg": round(float(np.mean(prod_adj[-n_recent:])), 3),
            "margin_push_avg": round(float(np.mean(margin_contrib[-n_recent:])), 3),
            "import_push_avg": round(float(np.mean(import_contrib[-n_recent:])), 3),
        }

        # Shares
        total = abs(decomposition["wage_push_avg"]) + abs(decomposition["margin_push_avg"]) + abs(decomposition["import_push_avg"])
        if total > 0:
            decomposition["wage_share"] = round(abs(decomposition["wage_push_avg"]) / total, 3)
            decomposition["margin_share"] = round(abs(decomposition["margin_push_avg"]) / total, 3)
            decomposition["import_share"] = round(abs(decomposition["import_push_avg"]) / total, 3)

        # Dominant driver
        shares = {
            "wage_push": abs(decomposition["wage_push_avg"]),
            "margin_push": abs(decomposition["margin_push_avg"]),
            "import_push": abs(decomposition["import_push_avg"]),
        }
        decomposition["dominant_driver"] = max(shares, key=shares.get)

        # Time series (subsampled)
        step = max(1, T // 50)
        decomposition["series"] = {
            "dates": [common[i] for i in range(0, T, step)],
            "wage_push": [round(float(prod_adj[i]), 3) for i in range(0, T, step)],
            "margin_push": [round(float(margin_contrib[i]), 3) for i in range(0, T, step)],
            "import_push": [round(float(import_contrib[i]), 3) for i in range(0, T, step)],
        }

        return decomposition

    @staticmethod
    def _rolling_correlation(x: np.ndarray, y: np.ndarray, window: int) -> np.ndarray:
        """Rolling Pearson correlation."""
        T = len(x)
        corr = np.zeros(T - window + 1)
        for t in range(T - window + 1):
            corr[t] = np.corrcoef(x[t:t + window], y[t:t + window])[0, 1]
        return corr

    @staticmethod
    def _compute_score(results: dict) -> float:
        """Score based on spiral risk."""
        score = 15.0  # baseline

        gc = results.get("granger_causality", {})
        direction = gc.get("spiral_direction", "no_feedback")

        if direction == "bidirectional_spiral":
            score += 35
        elif direction == "wage_push":
            score += 20
        elif direction == "cost_of_living":
            score += 15

        # High wage passthrough
        pe = results.get("price_equation", {})
        if pe.get("wage_passthrough", 0) > 0.7:
            score += 15
        elif pe.get("wage_passthrough", 0) > 0.4:
            score += 8

        # High inflation persistence
        if pe.get("inflation_persistence", 0) > 0.7:
            score += 10

        # Full wage indexation
        we = results.get("wage_equation", {})
        if we.get("full_indexation", False):
            score += 10

        # Negative real wage growth (squeeze)
        rw = results.get("real_wage", {})
        if rw.get("declining", False):
            score += 5

        return min(score, 100)
