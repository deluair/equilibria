"""Nowcasting - Bridge equations for GDP nowcasting, monthly-to-quarterly, dynamic factor model."""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class Nowcasting(LayerBase):
    layer_id = "l2"
    name = "Nowcasting"
    weight = 0.05

    # Monthly indicators for bridge equations
    MONTHLY_INDICATORS = {
        "ip": "INDPRO",              # Industrial Production
        "retail_sales": "RSAFS",     # Retail Sales
        "nonfarm_payrolls": "PAYEMS", # Total Nonfarm Payrolls
        "initial_claims": "ICSA",    # Initial Unemployment Claims
        "hours_worked": "AWHMAN",    # Avg Weekly Hours Manufacturing
        "ism_pmi": "MANEMP",         # ISM Manufacturing Employment
        "consumer_sentiment": "UMCSENT",  # U Michigan Consumer Sentiment
        "housing_starts": "HOUST",   # Housing Starts
        "permits": "PERMIT",         # Building Permits
        "personal_income": "PI",     # Personal Income
        "pce": "PCE",               # Personal Consumption Expenditures
        "durable_orders": "DGORDER",  # Durable Goods Orders
    }

    GDP_SERIES = "GDP"  # Quarterly GDP

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        lookback = kwargs.get("lookback_years", 15)
        n_factors = kwargs.get("n_factors", 3)

        # Fetch monthly indicators
        monthly_ids = list(self.MONTHLY_INDICATORS.values())
        monthly_rows = await db.execute_fetchall(
            """
            SELECT series_id, date, value FROM data_points
            WHERE series_id IN ({})
              AND country_code = ?
              AND date >= date('now', ?)
            ORDER BY series_id, date
            """.format(",".join("?" for _ in monthly_ids)),
            (*monthly_ids, country, f"-{lookback} years"),
        )

        # Fetch quarterly GDP
        gdp_rows = await db.execute_fetchall(
            """
            SELECT date, value FROM data_points
            WHERE series_id = ? AND country_code = ?
              AND date >= date('now', ?)
            ORDER BY date
            """,
            (self.GDP_SERIES, country, f"-{lookback} years"),
        )

        monthly_map: dict[str, dict[str, float]] = {}
        for r in monthly_rows:
            monthly_map.setdefault(r["series_id"], {})[r["date"]] = float(r["value"])

        gdp_map = {r["date"]: float(r["value"]) for r in gdp_rows}

        if not gdp_map:
            return {"score": 50.0, "results": {}, "note": "no GDP data"}

        results = {}

        # Method 1: Bridge equations (individual indicators -> GDP)
        bridge = self._bridge_equations(monthly_map, gdp_map)
        results["bridge_equations"] = bridge

        # Method 2: Dynamic Factor Model
        dfm = self._dynamic_factor_model(monthly_map, gdp_map, n_factors)
        results["dynamic_factor_model"] = dfm

        # Method 3: Simple average of bridge nowcasts
        nowcasts = []
        if bridge.get("nowcasts"):
            for name, nc in bridge["nowcasts"].items():
                if nc.get("nowcast") is not None:
                    nowcasts.append(nc["nowcast"])

        if dfm.get("nowcast") is not None:
            nowcasts.append(dfm["nowcast"])

        if nowcasts:
            combined_nowcast = float(np.mean(nowcasts))
            nowcast_std = float(np.std(nowcasts, ddof=1)) if len(nowcasts) > 1 else 0.0
        else:
            combined_nowcast = None
            nowcast_std = None

        results["combined"] = {
            "nowcast_gdp_growth": combined_nowcast,
            "nowcast_std": nowcast_std,
            "n_models": len(nowcasts),
            "confidence_interval_68": [
                combined_nowcast - nowcast_std,
                combined_nowcast + nowcast_std,
            ] if combined_nowcast is not None and nowcast_std is not None else None,
        }

        # Compare with last actual GDP
        gdp_dates = sorted(gdp_map.keys())
        if len(gdp_dates) >= 2:
            last_gdp = gdp_map[gdp_dates[-1]]
            prev_gdp = gdp_map[gdp_dates[-2]]
            last_growth = (last_gdp / prev_gdp - 1) * 400  # annualized quarterly growth
            results["last_actual"] = {
                "date": gdp_dates[-1],
                "gdp_growth_annualized": float(last_growth),
            }

            # Revision tracking: nowcast vs last actual
            if combined_nowcast is not None:
                results["revision_from_actual"] = float(combined_nowcast - last_growth)

        # Score: low growth nowcast = higher stress
        if combined_nowcast is not None:
            # Map: -4% -> 90 (deep contraction), 0% -> 50, +2% -> 30, +4% -> 15
            score = float(np.clip(50.0 - combined_nowcast * 10.0, 0, 100))
        else:
            score = 50.0

        return {
            "score": score,
            "results": results,
        }

    def _bridge_equations(self, monthly_map: dict, gdp_map: dict) -> dict:
        """Bridge equation nowcasts: regress quarterly GDP growth on
        quarterly-aggregated monthly indicators."""
        gdp_dates = sorted(gdp_map.keys())
        if len(gdp_dates) < 8:
            return {"note": "insufficient GDP data"}

        # Quarterly GDP growth (annualized)
        gdp_growth = []
        gdp_growth_dates = []
        for i in range(1, len(gdp_dates)):
            g = (gdp_map[gdp_dates[i]] / gdp_map[gdp_dates[i - 1]] - 1) * 400
            gdp_growth.append(g)
            gdp_growth_dates.append(gdp_dates[i])

        # For each monthly indicator, aggregate to quarterly and build bridge equation
        nowcasts = {}
        for name, sid in self.MONTHLY_INDICATORS.items():
            data = monthly_map.get(sid, {})
            if len(data) < 12:
                continue

            # Aggregate monthly to quarterly (average of 3 months)
            quarterly = self._monthly_to_quarterly(data)

            # Align with GDP growth
            common = sorted(set(gdp_growth_dates) & set(quarterly.keys()))
            if len(common) < 8:
                continue

            y = np.array([gdp_growth[gdp_growth_dates.index(d)] for d in common])
            x_levels = np.array([quarterly[d] for d in common])

            # Use growth rate of indicator
            if len(x_levels) < 3:
                continue
            x_growth = np.diff(x_levels) / np.maximum(np.abs(x_levels[:-1]), 1e-12) * 100
            y_aligned = y[1:]
            common[1:]

            if len(y_aligned) < 6:
                continue

            # OLS: gdp_growth = alpha + beta * indicator_growth
            X = np.column_stack([np.ones(len(y_aligned)), x_growth])
            beta = np.linalg.lstsq(X, y_aligned, rcond=None)[0]
            fitted = X @ beta
            residuals = y_aligned - fitted
            r_squared = 1 - np.sum(residuals ** 2) / np.sum((y_aligned - np.mean(y_aligned)) ** 2)

            # Nowcast using latest available monthly data
            sorted(data.keys())
            latest_quarterly = self._latest_quarterly_growth(data)
            if latest_quarterly is not None:
                nowcast = float(beta[0] + beta[1] * latest_quarterly)
            else:
                nowcast = None

            nowcasts[name] = {
                "nowcast": nowcast,
                "r_squared": float(r_squared),
                "beta": float(beta[1]),
                "alpha": float(beta[0]),
                "n_obs": len(y_aligned),
                "rmse": float(np.sqrt(np.mean(residuals ** 2))),
            }

        return {"nowcasts": nowcasts}

    def _dynamic_factor_model(self, monthly_map: dict, gdp_map: dict,
                              n_factors: int) -> dict:
        """Simplified dynamic factor model: extract common factors from monthly
        indicators, then use them to nowcast GDP growth."""
        # Build panel of monthly indicators (standardized growth rates)
        indicator_names = []
        indicator_data = {}

        for name, sid in self.MONTHLY_INDICATORS.items():
            data = monthly_map.get(sid, {})
            if len(data) >= 24:
                indicator_names.append(name)
                indicator_data[name] = data

        if len(indicator_names) < n_factors + 1:
            return {"note": "insufficient indicators for factor model"}

        # Align to common monthly dates
        all_dates = set()
        for data in indicator_data.values():
            all_dates |= set(data.keys())
        common_monthly = sorted(all_dates)

        # Build matrix, forward-fill missing
        n_dates = len(common_monthly)
        n_vars = len(indicator_names)
        panel = np.full((n_dates, n_vars), np.nan)

        for col, name in enumerate(indicator_names):
            data = indicator_data[name]
            for i, d in enumerate(common_monthly):
                if d in data:
                    panel[i, col] = data[d]

        # Forward-fill
        for col in range(n_vars):
            last = np.nan
            for row in range(n_dates):
                if np.isnan(panel[row, col]):
                    panel[row, col] = last
                else:
                    last = panel[row, col]

        # Drop rows with NaN
        valid = ~np.any(np.isnan(panel), axis=1)
        panel = panel[valid]
        valid_dates = [d for d, v in zip(common_monthly, valid) if v]

        if panel.shape[0] < 24:
            return {"note": "insufficient aligned data"}

        # Compute growth rates
        growth = np.diff(panel, axis=0) / np.maximum(np.abs(panel[:-1]), 1e-12) * 100
        growth_dates = valid_dates[1:]

        # Standardize
        means = np.mean(growth, axis=0)
        stds = np.std(growth, axis=0, ddof=1)
        stds[stds < 1e-12] = 1.0
        Z = (growth - means) / stds

        # PCA for factor extraction
        cov = np.cov(Z, rowvar=False)
        eigenvalues, eigenvectors = np.linalg.eigh(cov)
        idx = np.argsort(eigenvalues)[::-1]
        eigenvalues = eigenvalues[idx]
        eigenvectors = eigenvectors[:, idx]

        # Extract first n_factors
        n_factors = min(n_factors, n_vars)
        factors = Z @ eigenvectors[:, :n_factors]
        variance_explained = [
            float(eigenvalues[i] / np.sum(eigenvalues))
            for i in range(n_factors)
        ]

        # Aggregate factors to quarterly
        quarterly_factors = {}
        for i, d in enumerate(growth_dates):
            q = d[:7]  # year-month as quarter proxy
            quarterly_factors.setdefault(q, []).append(factors[i])

        q_dates = sorted(quarterly_factors.keys())
        q_factors = np.array([np.mean(quarterly_factors[q], axis=0) for q in q_dates])

        # Align with quarterly GDP growth
        gdp_dates = sorted(gdp_map.keys())
        gdp_growth = {}
        for i in range(1, len(gdp_dates)):
            g = (gdp_map[gdp_dates[i]] / gdp_map[gdp_dates[i - 1]] - 1) * 400
            gdp_growth[gdp_dates[i]] = g

        # Match quarterly factor dates to GDP dates (approximate by quarter)
        aligned_y = []
        aligned_f = []
        for i, qd in enumerate(q_dates):
            # Find closest GDP date
            for gd in gdp_dates:
                if gd[:7] == qd or (gd[:4] == qd[:4] and abs(int(gd[5:7]) - int(qd[5:7])) <= 1):
                    if gd in gdp_growth:
                        aligned_y.append(gdp_growth[gd])
                        aligned_f.append(q_factors[i])
                        break

        if len(aligned_y) < 8:
            return {"note": "insufficient aligned factor-GDP data"}

        Y = np.array(aligned_y)
        F = np.array(aligned_f)

        # OLS: GDP_growth = alpha + beta @ factors
        X = np.column_stack([np.ones(len(Y)), F])
        beta = np.linalg.lstsq(X, Y, rcond=None)[0]
        fitted = X @ beta
        residuals = Y - fitted
        r_squared = 1 - np.sum(residuals ** 2) / np.sum((Y - np.mean(Y)) ** 2)

        # Nowcast: use latest quarter's factors
        latest_factors = q_factors[-1]
        x_latest = np.concatenate([[1.0], latest_factors])
        nowcast = float(x_latest @ beta)

        # Factor loadings (which indicators load on which factors)
        loadings = {
            indicator_names[i]: [float(eigenvectors[i, j]) for j in range(n_factors)]
            for i in range(n_vars)
        }

        return {
            "nowcast": nowcast,
            "r_squared": float(r_squared),
            "n_factors": n_factors,
            "variance_explained": variance_explained,
            "factor_loadings": loadings,
            "n_obs": len(Y),
            "rmse": float(np.sqrt(np.mean(residuals ** 2))),
        }

    @staticmethod
    def _monthly_to_quarterly(data: dict[str, float]) -> dict[str, float]:
        """Aggregate monthly data to quarterly by averaging."""
        quarterly = {}
        by_quarter: dict[str, list[float]] = {}

        for d, v in sorted(data.items()):
            # Map to quarter start date
            year = d[:4]
            month = int(d[5:7]) if len(d) >= 7 else 1
            q_month = ((month - 1) // 3) * 3 + 1
            q_date = f"{year}-{q_month:02d}-01"
            by_quarter.setdefault(q_date, []).append(v)

        for q_date, vals in by_quarter.items():
            quarterly[q_date] = float(np.mean(vals))

        return quarterly

    @staticmethod
    def _latest_quarterly_growth(data: dict[str, float]) -> float | None:
        """Compute growth rate of latest quarter vs previous quarter."""
        dates = sorted(data.keys())
        if len(dates) < 6:
            return None

        # Last 3 months
        last_3 = [data[d] for d in dates[-3:]]
        prev_3 = [data[d] for d in dates[-6:-3]]

        avg_last = np.mean(last_3)
        avg_prev = np.mean(prev_3)

        if abs(avg_prev) < 1e-12:
            return None

        return float((avg_last / avg_prev - 1) * 100)
