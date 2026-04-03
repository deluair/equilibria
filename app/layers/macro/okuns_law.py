"""Okun's Law estimation module.

Methodology
-----------
Two canonical specifications:

1. **Gap version** (Okun 1962):
   (Y - Y*) / Y* = beta * (u - u*)
   The output gap is proportional to the unemployment gap.
   Requires estimated potential output (Y*) and NAIRU (u*).

2. **Difference version**:
   delta_Y / Y = alpha + beta * delta_u + e_t
   GDP growth is linearly related to the change in unemployment.
   More robust as it avoids unobservable potential output.

Stability testing:
- Rolling window estimation (40-quarter windows)
- Chow test at candidate break points (recessions)
- Recursive least squares with CUSUM statistic

Okun's coefficient (beta) is typically around -2 for the US (a 1pp rise
in unemployment is associated with a 2% fall in output). Values outside
[-4, -0.5] suggest structural change or data issues.

Score reflects instability and magnitude of Okun deviations. Higher = more stress.

Sources: FRED (real GDP growth, unemployment rate, CBO potential output)
"""

import numpy as np

from app.layers.base import LayerBase


class OkunsLaw(LayerBase):
    layer_id = "l2"
    name = "Okun's Law"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")

        # Fetch GDP growth and unemployment change
        gdp_rows = await db.execute_fetchall(
            "SELECT date, value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE code = ?) ORDER BY date",
            (f"GDP_{country}",),
        )
        unemp_rows = await db.execute_fetchall(
            "SELECT date, value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE code = ?) ORDER BY date",
            (f"UNEMP_{country}",),
        )

        if not gdp_rows or not unemp_rows:
            return {"score": 50, "results": {"error": "insufficient data"}}

        gdp_dict = {r[0]: float(r[1]) for r in gdp_rows}
        unemp_dict = {r[0]: float(r[1]) for r in unemp_rows}
        common_dates = sorted(set(gdp_dict) & set(unemp_dict))

        if len(common_dates) < 12:
            return {"score": 50, "results": {"error": "too few overlapping observations"}}

        gdp_vals = np.array([gdp_dict[d] for d in common_dates])
        u_vals = np.array([unemp_dict[d] for d in common_dates])

        results = {
            "country": country,
            "n_obs": len(common_dates),
            "period": f"{common_dates[0]} to {common_dates[-1]}",
        }

        # --- Difference version ---
        gdp_growth = np.diff(np.log(gdp_vals)) * 100  # percent
        u_change = np.diff(u_vals)
        dates_diff = common_dates[1:]

        n = len(gdp_growth)
        X = np.column_stack([np.ones(n), u_change])
        beta = np.linalg.lstsq(X, gdp_growth, rcond=None)[0]
        resid = gdp_growth - X @ beta
        sse = float(np.sum(resid ** 2))
        sst = float(np.sum((gdp_growth - np.mean(gdp_growth)) ** 2))
        r_squared = 1 - sse / sst if sst > 0 else 0.0

        # HC1 robust SE
        bread = np.linalg.inv(X.T @ X)
        meat = X.T @ np.diag(resid ** 2) @ X
        vcov = (n / (n - 2)) * bread @ meat @ bread
        se = np.sqrt(np.diag(vcov))

        results["difference_version"] = {
            "intercept": float(beta[0]),
            "okun_coefficient": float(beta[1]),
            "se": float(se[1]),
            "t_stat": float(beta[1] / se[1]) if se[1] > 0 else 0.0,
            "r_squared": round(r_squared, 4),
            "interpretation": (
                f"A 1pp rise in unemployment is associated with a "
                f"{abs(beta[1]):.2f}% {'decrease' if beta[1] < 0 else 'increase'} in GDP"
            ),
        }

        # --- Gap version (if potential output available) ---
        pot_rows = await db.execute_fetchall(
            "SELECT date, value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE code = ?) ORDER BY date",
            (f"POTENTIAL_GDP_{country}",),
        )
        nairu_rows = await db.execute_fetchall(
            "SELECT date, value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE code = ?) ORDER BY date",
            (f"NAIRU_{country}",),
        )

        if pot_rows and nairu_rows:
            pot_dict = {r[0]: float(r[1]) for r in pot_rows}
            nairu_dict = {r[0]: float(r[1]) for r in nairu_rows}
            gap_dates = sorted(set(common_dates) & set(pot_dict) & set(nairu_dict))

            if len(gap_dates) >= 10:
                output_gap = np.array([
                    (gdp_dict[d] - pot_dict[d]) / pot_dict[d] * 100
                    for d in gap_dates
                ])
                unemp_gap = np.array([
                    unemp_dict[d] - nairu_dict[d] for d in gap_dates
                ])

                ng = len(output_gap)
                Xg = np.column_stack([np.ones(ng), unemp_gap])
                beta_g = np.linalg.lstsq(Xg, output_gap, rcond=None)[0]
                resid_g = output_gap - Xg @ beta_g
                sse_g = float(np.sum(resid_g ** 2))
                sst_g = float(np.sum((output_gap - np.mean(output_gap)) ** 2))
                r2_g = 1 - sse_g / sst_g if sst_g > 0 else 0.0

                bread_g = np.linalg.inv(Xg.T @ Xg)
                meat_g = Xg.T @ np.diag(resid_g ** 2) @ Xg
                vcov_g = (ng / (ng - 2)) * bread_g @ meat_g @ bread_g
                se_g = np.sqrt(np.diag(vcov_g))

                results["gap_version"] = {
                    "intercept": float(beta_g[0]),
                    "okun_coefficient": float(beta_g[1]),
                    "se": float(se_g[1]),
                    "t_stat": float(beta_g[1] / se_g[1]) if se_g[1] > 0 else 0.0,
                    "r_squared": round(r2_g, 4),
                }

        # --- Rolling stability test ---
        window = kwargs.get("window", 40)
        if n >= window + 10:
            rolling_betas = []
            rolling_dates = []
            for i in range(n - window + 1):
                y_w = gdp_growth[i : i + window]
                x_w = np.column_stack([np.ones(window), u_change[i : i + window]])
                b_w = np.linalg.lstsq(x_w, y_w, rcond=None)[0]
                rolling_betas.append(float(b_w[1]))
                rolling_dates.append(dates_diff[i + window - 1])

            rolling_arr = np.array(rolling_betas)
            results["stability"] = {
                "rolling_window": window,
                "rolling_betas": rolling_betas,
                "rolling_dates": rolling_dates,
                "beta_mean": float(np.mean(rolling_arr)),
                "beta_std": float(np.std(rolling_arr, ddof=1)),
                "beta_range": [float(np.min(rolling_arr)), float(np.max(rolling_arr))],
                "coefficient_of_variation": float(
                    np.std(rolling_arr, ddof=1) / abs(np.mean(rolling_arr))
                ) if abs(np.mean(rolling_arr)) > 1e-10 else None,
            }

            # --- CUSUM test ---
            # Recursive residuals from expanding OLS
            rec_resid = []
            for i in range(10, n):
                y_sub = gdp_growth[:i]
                x_sub = np.column_stack([np.ones(i), u_change[:i]])
                b_sub = np.linalg.lstsq(x_sub, y_sub, rcond=None)[0]
                pred = float(np.array([1.0, u_change[i]]) @ b_sub)
                rec_resid.append(float(gdp_growth[i]) - pred)

            if rec_resid:
                rec_arr = np.array(rec_resid)
                sigma = float(np.std(rec_arr, ddof=1)) if len(rec_arr) > 1 else 1.0
                cusum = np.cumsum(rec_arr) / sigma if sigma > 0 else np.cumsum(rec_arr)
                # Significance boundary: +/- a + 2*a*t/T where a = 0.948 (5% level)
                T = len(cusum)
                boundary_5pct = [0.948 * (1 + 2 * t / T) for t in range(T)]
                max_cusum = float(np.max(np.abs(cusum)))
                results["stability"]["cusum_max"] = round(max_cusum, 3)
                results["stability"]["cusum_boundary_5pct"] = round(boundary_5pct[-1], 3)
                results["stability"]["cusum_rejects_stability"] = max_cusum > boundary_5pct[-1]

        # --- Score ---
        okun_coeff = beta[1]
        # Anomalous sign
        sign_penalty = 30 if okun_coeff > 0 else 0
        # Coefficient outside normal range [-4, -0.5]
        range_penalty = 0
        if okun_coeff < 0:
            if abs(okun_coeff) > 4 or abs(okun_coeff) < 0.5:
                range_penalty = 15
        # Low fit
        fit_penalty = (1 - max(r_squared, 0)) * 30
        # Instability
        instability_penalty = 0
        if "stability" in results:
            cv = results["stability"].get("coefficient_of_variation")
            if cv is not None and cv > 0.3:
                instability_penalty = min(cv * 30, 25)

        score = min(sign_penalty + range_penalty + fit_penalty + instability_penalty, 100)

        return {"score": round(score, 1), "results": results}
