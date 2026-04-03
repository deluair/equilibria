"""Wage Phillips curve estimation.

The original Phillips (1958) curve relates nominal wage growth to unemployment:
    dW/W = a + b*(1/U)

The modern wage Phillips curve (linear approximation):
    dw_t = a + b*u_t + c*pi_t^e + e_t

where dw is nominal wage growth, u is unemployment rate, and pi^e is expected
inflation. The expectations-augmented version (Friedman 1968, Phelps 1967)
implies no long-run trade-off: b captures short-run slope.

Flattening hypothesis (post-2008):
    The Phillips curve slope |b| has declined since the 1990s (Blanchard 2016).
    Possible explanations: anchored expectations, globalization, labor market
    composition changes, measurement issues.

Estimation:
    1. Rolling-window OLS to track slope over time
    2. Structural break test (Chow or sup-Wald) for GFC
    3. Compare pre-2008 vs post-2008 slopes

Nonlinearity (Debelle & Laxton 1997):
    Slope may be steeper at low unemployment (convex Phillips curve).

References:
    Phillips, A.W. (1958). The Relation Between Unemployment and the Rate
        of Change of Money Wage Rates in the United Kingdom, 1861-1957.
        Economica 25(100): 283-299.
    Blanchard, O. (2016). The Phillips Curve: Back to the '60s? AEA P&P.
    Hooper, P., Mishkin, F. & Sufi, A. (2020). Prospects for Inflation in
        a High Pressure Economy: Is the Phillips Curve Dead or Is It Just
        Hibernating? Research in Economics 74(1).

Score: flat slope (|b| < 0.2) -> WATCH (unanchored labor market signal).
Steep negative slope (|b| > 0.5) -> STABLE (responsive). Very steep -> STRESS
(high sensitivity to demand shocks).
"""

import numpy as np

from app.layers.base import LayerBase


class WagePhillipsCurve(LayerBase):
    layer_id = "l3"
    name = "Wage Phillips Curve"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'wage_phillips'
            ORDER BY dp.date ASC
            """,
            (country,),
        )

        if not rows or len(rows) < 10:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient Phillips curve data"}

        import json

        dates = []
        wage_growth = []
        unemployment = []
        inflation_exp = []

        for row in rows:
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            wg = row["value"]
            u = meta.get("unemployment_rate")
            pi_e = meta.get("expected_inflation")
            if wg is None or u is None:
                continue
            dates.append(row["date"])
            wage_growth.append(float(wg))
            unemployment.append(float(u))
            inflation_exp.append(float(pi_e) if pi_e is not None else 0.0)

        n = len(wage_growth)
        if n < 10:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient valid obs"}

        dw = np.array(wage_growth)
        u_arr = np.array(unemployment)
        pi_e = np.array(inflation_exp)

        # Expectations-augmented Phillips curve
        X = np.column_stack([np.ones(n), u_arr, pi_e])
        beta = np.linalg.lstsq(X, dw, rcond=None)[0]
        resid = dw - X @ beta
        ss_res = np.sum(resid ** 2)
        ss_tot = np.sum((dw - dw.mean()) ** 2)
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

        # Robust SE
        n_k = n - X.shape[1]
        XtX_inv = np.linalg.pinv(X.T @ X)
        omega = np.diag(resid ** 2) * (n / max(n_k, 1))
        V = XtX_inv @ (X.T @ omega @ X) @ XtX_inv
        se = np.sqrt(np.maximum(np.diag(V), 0.0))

        phillips_slope = float(beta[1])
        inflation_passthrough = float(beta[2])

        # Rolling window estimation for flattening test
        window = min(20, n // 2)
        rolling_slopes = []
        if n >= window + 5:
            for start in range(n - window + 1):
                end = start + window
                X_w = np.column_stack([np.ones(window), u_arr[start:end], pi_e[start:end]])
                beta_w = np.linalg.lstsq(X_w, dw[start:end], rcond=None)[0]
                rolling_slopes.append({
                    "period_end": dates[end - 1] if end - 1 < len(dates) else None,
                    "slope": round(float(beta_w[1]), 4),
                })

        # Flattening: compare early vs late slope
        flattening = None
        if n >= 20:
            mid = n // 2
            X_early = np.column_stack([np.ones(mid), u_arr[:mid], pi_e[:mid]])
            X_late = np.column_stack([np.ones(n - mid), u_arr[mid:], pi_e[mid:]])
            beta_early = np.linalg.lstsq(X_early, dw[:mid], rcond=None)[0]
            beta_late = np.linalg.lstsq(X_late, dw[mid:], rcond=None)[0]
            slope_early = float(beta_early[1])
            slope_late = float(beta_late[1])
            flattening = {
                "early_slope": round(slope_early, 4),
                "late_slope": round(slope_late, 4),
                "change": round(slope_late - slope_early, 4),
                "flattened": abs(slope_late) < abs(slope_early) * 0.7,
            }

        # NAIRU estimate: wage growth = expected inflation when dw = pi_e
        # b0 + b1*u* + b2*pi_e = pi_e => u* = -b0 / b1 (if b2 ~ 1)
        nairu = -beta[0] / beta[1] if abs(beta[1]) > 1e-6 else None

        # Score: flat curve is concerning (unresponsive market)
        abs_slope = abs(phillips_slope)
        if abs_slope < 0.1:
            score = 55.0  # very flat = disconnected
        elif abs_slope < 0.3:
            score = 35.0 + (0.3 - abs_slope) * 100.0
        elif abs_slope > 1.0:
            score = 50.0 + (abs_slope - 1.0) * 30.0  # too steep = volatile
        else:
            score = 15.0 + (abs_slope - 0.3) * 28.0
        score = max(0.0, min(100.0, score))

        coef_names = ["constant", "unemployment_rate", "expected_inflation"]

        result = {
            "score": round(score, 2),
            "country": country,
            "n_obs": n,
            "coefficients": dict(zip(coef_names, beta.tolist())),
            "std_errors": dict(zip(coef_names, se.tolist())),
            "r_squared": round(r2, 4),
            "phillips_slope": round(phillips_slope, 4),
            "inflation_passthrough": round(inflation_passthrough, 4),
            "nairu_estimate": round(nairu, 2) if nairu is not None and 0 < nairu < 30 else None,
            "time_range": {
                "start": dates[0] if dates else None,
                "end": dates[-1] if dates else None,
            },
        }

        if flattening:
            result["flattening_test"] = flattening
        if rolling_slopes:
            result["rolling_slopes"] = rolling_slopes[-5:]  # last 5 windows

        return result
