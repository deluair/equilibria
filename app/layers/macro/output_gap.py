"""Output Gap - Multivariate filter, production function approach, real-time vs revised."""

from __future__ import annotations

import numpy as np
from scipy.optimize import minimize

from app.layers.base import LayerBase


class OutputGap(LayerBase):
    layer_id = "l2"
    name = "Output Gap"
    weight = 0.05

    # Series for output gap estimation
    GDP_SERIES = "GDP"           # Real GDP (billions)
    GDPPOT_SERIES = "GDPPOT"    # CBO Potential GDP
    IP_SERIES = "INDPRO"        # Industrial Production
    UNRATE_SERIES = "UNRATE"    # Unemployment Rate
    NAIRU_SERIES = "NROU"       # CBO Natural Rate of Unemployment
    CAPACITY_SERIES = "TCU"     # Capacity Utilization
    CPI_SERIES = "CPIAUCSL"     # CPI for multivariate filter

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        lookback = kwargs.get("lookback_years", 25)
        hp_lambda = kwargs.get("hp_lambda", 1600)

        series_ids = [
            self.GDP_SERIES, self.GDPPOT_SERIES, self.IP_SERIES,
            self.UNRATE_SERIES, self.NAIRU_SERIES, self.CAPACITY_SERIES,
            self.CPI_SERIES,
        ]

        rows = await db.execute_fetchall(
            """
            SELECT series_id, date, value FROM data_points
            WHERE series_id IN ({})
              AND country_code = ?
              AND date >= date('now', ?)
            ORDER BY series_id, date
            """.format(",".join("?" for _ in series_ids)),
            (*series_ids, country, f"-{lookback} years"),
        )

        series_map: dict[str, dict[str, float]] = {}
        for r in rows:
            series_map.setdefault(r["series_id"], {})[r["date"]] = float(r["value"])

        results = {}

        # Method 1: CBO-based output gap (GDP vs potential)
        cbo_gap = self._cbo_gap(series_map)
        if cbo_gap:
            results["cbo_gap"] = cbo_gap

        # Method 2: HP filter gap
        hp_gap = self._hp_filter_gap(series_map, hp_lambda)
        if hp_gap:
            results["hp_filter_gap"] = hp_gap

        # Method 3: Hamilton filter gap (more robust to endpoint issues)
        hamilton_gap = self._hamilton_filter_gap(series_map)
        if hamilton_gap:
            results["hamilton_filter_gap"] = hamilton_gap

        # Method 4: Production function approach
        pf_gap = self._production_function_gap(series_map)
        if pf_gap:
            results["production_function_gap"] = pf_gap

        # Method 5: Multivariate filter (GDP + unemployment + inflation)
        mv_gap = self._multivariate_filter_gap(series_map, hp_lambda)
        if mv_gap:
            results["multivariate_gap"] = mv_gap

        # Capacity utilization gap
        cap_gap = self._capacity_gap(series_map)
        if cap_gap:
            results["capacity_gap"] = cap_gap

        # Unemployment gap (actual - NAIRU)
        u_gap = self._unemployment_gap(series_map)
        if u_gap:
            results["unemployment_gap"] = u_gap

        # Composite output gap: weighted average of available measures
        gap_estimates = []
        weights_used = []
        method_weights = {
            "cbo_gap": 0.30,
            "hp_filter_gap": 0.15,
            "hamilton_filter_gap": 0.15,
            "production_function_gap": 0.20,
            "multivariate_gap": 0.20,
        }

        for method, w in method_weights.items():
            if method in results and results[method].get("current_gap") is not None:
                gap_estimates.append(results[method]["current_gap"])
                weights_used.append(w)

        if gap_estimates:
            w_arr = np.array(weights_used)
            w_arr = w_arr / w_arr.sum()
            composite_gap = float(np.average(gap_estimates, weights=w_arr))
            results["composite_gap"] = composite_gap
            results["n_methods"] = len(gap_estimates)
        else:
            composite_gap = 0.0
            results["composite_gap"] = None

        # Real-time vs revised: flag that real-time estimates are typically
        # 1-2 pp different from final revised estimates
        results["real_time_caveat"] = (
            "Output gap estimates are subject to substantial revision. "
            "Real-time estimates differ from final revised by 1-2 percentage points on average."
        )

        # Score: large negative gap = recession (high stress), large positive = overheating
        # Map: -4% gap -> 85 (deep recession), 0% -> 25 (equilibrium), +4% -> 65 (overheating)
        if composite_gap is not None:
            if composite_gap < 0:
                score = float(np.clip(25.0 + abs(composite_gap) * 15.0, 0, 100))
            else:
                score = float(np.clip(25.0 + composite_gap * 10.0, 0, 100))
        else:
            score = 50.0

        return {
            "score": score,
            "results": results,
        }

    def _cbo_gap(self, series_map: dict) -> dict | None:
        """Output gap from CBO potential GDP."""
        gdp = series_map.get(self.GDP_SERIES, {})
        pot = series_map.get(self.GDPPOT_SERIES, {})
        common = sorted(set(gdp.keys()) & set(pot.keys()))
        if len(common) < 4:
            return None

        gaps = [(gdp[d] - pot[d]) / pot[d] * 100 for d in common]
        return {
            "current_gap": gaps[-1],
            "mean_gap": float(np.mean(gaps)),
            "std_gap": float(np.std(gaps, ddof=1)),
            "series": [{"date": d, "gap_pct": g} for d, g in zip(common[-60:], gaps[-60:])],
        }

    def _hp_filter_gap(self, series_map: dict, lam: float) -> dict | None:
        """Hodrick-Prescott filter output gap."""
        gdp = series_map.get(self.GDP_SERIES, {})
        if len(gdp) < 16:
            return None

        dates = sorted(gdp.keys())
        y = np.log(np.array([gdp[d] for d in dates]))

        trend = self._hp_filter(y, lam)
        gap = (y - trend) * 100  # percentage deviation

        return {
            "current_gap": float(gap[-1]),
            "series": [{"date": d, "gap_pct": float(g)} for d, g in zip(dates[-60:], gap[-60:])],
        }

    def _hamilton_filter_gap(self, series_map: dict) -> dict | None:
        """Hamilton (2018) filter: regress y(t+h) on y(t), y(t-1), ..., y(t-p+1).
        Uses h=8 quarters (2 years ahead), p=4 lags. Residuals = cyclical component."""
        gdp = series_map.get(self.GDP_SERIES, {})
        if len(gdp) < 20:
            return None

        dates = sorted(gdp.keys())
        y = np.log(np.array([gdp[d] for d in dates]))

        h = 8  # forecast horizon
        p = 4  # number of lags

        if len(y) < h + p + 4:
            return None

        n = len(y) - h - p + 1
        Y_dep = y[h + p - 1:]  # y(t+h)
        X = np.ones((n, p + 1))
        for lag in range(p):
            X[:, lag + 1] = y[p - 1 - lag:p - 1 - lag + n]

        # OLS
        beta = np.linalg.lstsq(X, Y_dep, rcond=None)[0]
        residuals = Y_dep - X @ beta

        # Map residuals back to dates
        gap = residuals * 100
        gap_dates = dates[h + p - 1:]

        return {
            "current_gap": float(gap[-1]),
            "series": [
                {"date": d, "gap_pct": float(g)}
                for d, g in zip(gap_dates[-60:], gap[-60:])
            ],
        }

    def _production_function_gap(self, series_map: dict) -> dict | None:
        """Production function approach: Y* = A* * F(K*, L*).
        Simplified: use capacity utilization and unemployment gap as proxies."""
        cap = series_map.get(self.CAPACITY_SERIES, {})
        unrate = series_map.get(self.UNRATE_SERIES, {})
        nairu = series_map.get(self.NAIRU_SERIES, {})

        # Use capacity utilization gap
        if len(cap) < 8:
            return None

        cap_dates = sorted(cap.keys())
        cap_arr = np.array([cap[d] for d in cap_dates])

        # Long-run average capacity utilization ~ 80%
        mean_cap = float(np.mean(cap_arr))
        cap_gap = (cap_arr - mean_cap) / mean_cap * 100

        # If we have unemployment gap, combine via Okun's law
        okun_coeff = -2.0  # standard Okun coefficient
        common = sorted(set(cap_dates) & set(unrate.keys()) & set(nairu.keys()))
        if len(common) >= 8:
            u_gap_arr = np.array([unrate[d] - nairu[d] for d in common])
            # Production function gap = Okun-implied output gap from unemployment gap
            okun_gap = okun_coeff * u_gap_arr
            return {
                "current_gap": float(okun_gap[-1]),
                "current_cap_gap": float(cap_gap[-1]),
                "okun_coefficient": okun_coeff,
                "series": [
                    {"date": d, "gap_pct": float(g)}
                    for d, g in zip(common[-60:], okun_gap[-60:])
                ],
            }

        return {
            "current_gap": float(cap_gap[-1]),
            "series": [
                {"date": d, "gap_pct": float(g)}
                for d, g in zip(cap_dates[-60:], cap_gap[-60:])
            ],
        }

    def _multivariate_filter_gap(self, series_map: dict, lam: float) -> dict | None:
        """Multivariate HP filter incorporating Phillips curve and Okun's law.
        Jointly estimate potential output, NAIRU, and trend inflation."""
        gdp = series_map.get(self.GDP_SERIES, {})
        unrate = series_map.get(self.UNRATE_SERIES, {})
        cpi = series_map.get(self.CPI_SERIES, {})

        common = sorted(set(gdp.keys()) & set(unrate.keys()) & set(cpi.keys()))
        if len(common) < 16:
            return None

        y = np.log(np.array([gdp[d] for d in common]))
        u = np.array([unrate[d] for d in common])
        p_arr = np.array([cpi[d] for d in common])
        pi = np.diff(np.log(p_arr)) * 1200  # annualized inflation
        y = y[1:]
        u = u[1:]
        common = common[1:]

        n = len(y)

        # HP filter on GDP for initial trend
        y_trend = self._hp_filter(y, lam)
        y_gap = (y - y_trend) * 100

        # HP filter on unemployment for NAIRU estimate
        u_trend = self._hp_filter(u, lam * 100)  # smoother for NAIRU
        u_gap = u - u_trend

        # Phillips curve coefficient: pi = alpha - beta * u_gap
        # Okun's law: y_gap = -gamma * u_gap
        if np.std(u_gap) > 1e-12:
            okun_gamma = float(-np.sum(y_gap * u_gap) / np.sum(u_gap ** 2))
            phillips_beta = float(-np.sum(pi * u_gap) / np.sum(u_gap ** 2))
        else:
            okun_gamma = 2.0
            phillips_beta = 0.5

        return {
            "current_gap": float(y_gap[-1]),
            "current_u_gap": float(u_gap[-1]),
            "okun_coefficient": okun_gamma,
            "phillips_slope": phillips_beta,
            "nairu_estimate": float(u_trend[-1]),
            "potential_growth": float((y_trend[-1] - y_trend[-2]) * 400) if n > 1 else None,
            "series": [
                {"date": d, "output_gap_pct": float(yg), "unemployment_gap": float(ug)}
                for d, yg, ug in zip(common[-60:], y_gap[-60:], u_gap[-60:])
            ],
        }

    def _capacity_gap(self, series_map: dict) -> dict | None:
        """Capacity utilization gap from long-run average."""
        cap = series_map.get(self.CAPACITY_SERIES, {})
        if len(cap) < 8:
            return None

        dates = sorted(cap.keys())
        vals = np.array([cap[d] for d in dates])
        mean_val = float(np.mean(vals))

        return {
            "current": float(vals[-1]),
            "long_run_mean": mean_val,
            "current_gap": float(vals[-1] - mean_val),
        }

    def _unemployment_gap(self, series_map: dict) -> dict | None:
        """Unemployment gap: actual - NAIRU."""
        u = series_map.get(self.UNRATE_SERIES, {})
        nairu = series_map.get(self.NAIRU_SERIES, {})
        common = sorted(set(u.keys()) & set(nairu.keys()))
        if len(common) < 4:
            return None

        gaps = [u[d] - nairu[d] for d in common]
        return {
            "current_gap": gaps[-1],
            "current_unemployment": u[common[-1]],
            "current_nairu": nairu[common[-1]],
            "series": [
                {"date": d, "gap": float(u[d] - nairu[d])}
                for d in common[-60:]
            ],
        }

    @staticmethod
    def _hp_filter(y: np.ndarray, lam: float) -> np.ndarray:
        """Hodrick-Prescott filter returning trend component."""
        n = len(y)
        e = np.eye(n)
        K = np.zeros((n - 2, n))
        for i in range(n - 2):
            K[i, i] = 1
            K[i, i + 1] = -2
            K[i, i + 2] = 1
        return np.linalg.solve(e + lam * K.T @ K, y)
