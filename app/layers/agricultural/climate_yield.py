"""Climate-yield panel regression and damage function projections.

Estimates the relationship between crop yields and climate variables
(temperature, precipitation, growing degree days) using panel regression,
then projects future yield impacts under climate change scenarios.

Methodology:
    Panel regression (fixed effects) of log yields on climate variables:

        ln(y_it) = alpha_i + beta_1*T_it + beta_2*T_it^2 + beta_3*P_it
                   + beta_4*P_it^2 + beta_5*GDD_it + gamma*t + e_it

    where i indexes regions/countries and t indexes years. The quadratic
    temperature term captures the nonlinear Schlenker-Roberts damage
    function: yields increase with temperature up to an optimum, then
    decline sharply.

    The Schlenker-Roberts (2009) damage function:
        y(T) = y_max * exp(-((T - T_opt) / sigma)^2)

    Climate impact score is based on projected yield loss under +2C warming.
    Higher score indicates greater vulnerability.

References:
    Schlenker, W. & Roberts, M.J. (2009). "Nonlinear temperature effects
        indicate severe damages to US crop yields under climate change."
        PNAS, 106(37), 15594-15598.
    Lobell, D.B., Schlenker, W. & Costa-Roberts, J. (2011). "Climate trends
        and global crop production since 1980." Science, 333(6042), 616-620.
    Burke, M. & Lobell, D.B. (2017). "Satellite-based assessment of yield
        variation and its determinants in smallholder African systems."
        PNAS, 114(9), 2189-2194.
"""

from __future__ import annotations

import numpy as np
from scipy import optimize

from app.layers.base import LayerBase


class ClimateYield(LayerBase):
    layer_id = "l5"
    name = "Climate-Yield Impact"

    # Default warming scenarios (degrees C above baseline)
    WARMING_SCENARIOS = [1.0, 1.5, 2.0, 3.0, 4.0]

    async def compute(self, db, **kwargs) -> dict:
        """Run climate-yield panel regression and project damage.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
            crop : str - crop name filter (default all crops)
            warming_delta : float - degrees C for projection (default 2.0)
        """
        country = kwargs.get("country_iso3", "BGD")
        crop = kwargs.get("crop")
        warming_delta = kwargs.get("warming_delta", 2.0)

        # Fetch yield-climate panel data
        crop_clause = "AND ds.description LIKE '%' || ? || '%'" if crop else ""
        params = [country]
        if crop:
            params.append(crop)

        rows = await db.fetch_all(
            f"""
            SELECT dp.date AS year, dp.value AS yield_val,
                   ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source = 'agricultural_climate'
              AND ds.country_iso3 = ?
              {crop_clause}
            ORDER BY dp.date
            """,
            tuple(params),
        )

        if not rows or len(rows) < 15:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient climate-yield panel data"}

        import json

        years = []
        yields = []
        temps = []
        precips = []
        gdds = []
        region_ids = []

        for row in rows:
            y_val = row["yield_val"]
            if y_val is None or y_val <= 0:
                continue
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            temp = meta.get("temperature_mean")
            prec = meta.get("precipitation_mm")
            gdd = meta.get("growing_degree_days")
            if temp is None or prec is None:
                continue

            years.append(int(row["year"][:4]) if isinstance(row["year"], str) else int(row["year"]))
            yields.append(float(y_val))
            temps.append(float(temp))
            precips.append(float(prec))
            gdds.append(float(gdd) if gdd is not None else 0.0)
            region_ids.append(meta.get("region_id", 0))

        n = len(yields)
        if n < 15:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient valid observations"}

        y = np.log(np.array(yields))
        T = np.array(temps)
        P = np.array(precips)
        G = np.array(gdds)
        t = np.array(years, dtype=float)
        t_norm = t - t.mean()
        regions = np.array(region_ids)

        # Panel fixed effects via demeaning (within estimator)
        unique_regions = np.unique(regions)
        if len(unique_regions) > 1:
            y_dm, T_dm, T2_dm, P_dm, P2_dm, G_dm, t_dm = self._demean(
                y, T, T ** 2, P, P ** 2, G, t_norm, groups=regions
            )
        else:
            # No panel structure, use raw (OLS)
            y_dm = y - y.mean()
            T_dm = T - T.mean()
            T2_dm = T ** 2 - (T ** 2).mean()
            P_dm = P - P.mean()
            P2_dm = P ** 2 - (P ** 2).mean()
            G_dm = G - G.mean()
            t_dm = t_norm - t_norm.mean()

        # Build design matrix [T, T^2, P, P^2, GDD, trend]
        X = np.column_stack([T_dm, T2_dm, P_dm, P2_dm, G_dm, t_dm])

        # OLS on demeaned data
        beta, residuals, rank, sv = np.linalg.lstsq(X, y_dm, rcond=None)
        fitted = X @ beta
        ss_res = float(np.sum((y_dm - fitted) ** 2))
        ss_tot = float(np.sum((y_dm - y_dm.mean()) ** 2))
        r2_within = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

        # Standard errors (HC1 robust)
        n_obs, k = X.shape
        dof = n_obs - k - len(unique_regions)
        XtX_inv = np.linalg.pinv(X.T @ X)
        resid = y_dm - fitted
        scale = n_obs / max(dof, 1)
        omega = np.diag(resid ** 2) * scale
        V = XtX_inv @ (X.T @ omega @ X) @ XtX_inv
        se = np.sqrt(np.maximum(np.diag(V), 0.0))

        coef_names = ["temperature", "temperature_sq", "precipitation",
                      "precipitation_sq", "gdd", "trend"]
        coefficients = dict(zip(coef_names, beta.tolist()))
        std_errors = dict(zip(coef_names, se.tolist()))

        # Optimal temperature (from quadratic: dy/dT = b1 + 2*b2*T = 0)
        b_temp = beta[0]
        b_temp_sq = beta[1]
        if b_temp_sq < 0:
            T_opt = -b_temp / (2 * b_temp_sq)
        else:
            T_opt = float(T.mean())

        # Schlenker-Roberts damage function fit
        sr_params = self._fit_schlenker_roberts(T, np.array(yields))

        # Project yield change under warming scenarios
        T_baseline = float(T.mean())
        projections = {}
        for delta in self.WARMING_SCENARIOS:
            T_new = T_baseline + delta
            # Quadratic panel prediction
            dT = delta
            yield_change_pct = (b_temp * dT + b_temp_sq * dT ** 2) * 100.0
            # Schlenker-Roberts prediction
            if sr_params is not None:
                y_base_sr = self._sr_function(T_baseline, *sr_params)
                y_new_sr = self._sr_function(T_new, *sr_params)
                sr_change_pct = ((y_new_sr - y_base_sr) / y_base_sr) * 100.0 if y_base_sr > 0 else 0.0
            else:
                sr_change_pct = None
            projections[f"+{delta}C"] = {
                "panel_yield_change_pct": round(float(yield_change_pct), 2),
                "sr_yield_change_pct": round(float(sr_change_pct), 2) if sr_change_pct is not None else None,
            }

        # Score: based on projected yield loss under specified warming
        target_proj = projections.get(f"+{warming_delta}C", {})
        panel_loss = target_proj.get("panel_yield_change_pct", 0.0)
        sr_loss = target_proj.get("sr_yield_change_pct")
        primary_loss = sr_loss if sr_loss is not None else panel_loss

        # Map yield loss to score: 0% loss = 0, -30% or worse = 100
        score = float(np.clip(-primary_loss / 30.0 * 100.0, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "n_obs": n,
            "n_regions": len(unique_regions),
            "panel_regression": {
                "coefficients": coefficients,
                "std_errors": std_errors,
                "r2_within": round(r2_within, 4),
                "optimal_temperature_c": round(float(T_opt), 2),
                "baseline_mean_temp_c": round(T_baseline, 2),
            },
            "schlenker_roberts": {
                "y_max": round(float(sr_params[0]), 2) if sr_params is not None else None,
                "T_opt": round(float(sr_params[1]), 2) if sr_params is not None else None,
                "sigma": round(float(sr_params[2]), 2) if sr_params is not None else None,
            },
            "projections": projections,
            "warming_scenario_used": f"+{warming_delta}C",
            "projected_yield_change_pct": round(float(primary_loss), 2),
        }

    @staticmethod
    def _demean(*arrays: np.ndarray, groups: np.ndarray):
        """Within-group demeaning for fixed effects estimation."""
        result = []
        for arr in arrays:
            demeaned = arr.copy()
            for g in np.unique(groups):
                mask = groups == g
                demeaned[mask] -= arr[mask].mean()
            result.append(demeaned)
        return result

    @staticmethod
    def _sr_function(T: float, y_max: float, T_opt: float, sigma: float) -> float:
        """Schlenker-Roberts Gaussian damage function."""
        return y_max * np.exp(-((T - T_opt) / sigma) ** 2)

    @staticmethod
    def _fit_schlenker_roberts(
        temps: np.ndarray, yields: np.ndarray
    ) -> tuple[float, float, float] | None:
        """Fit Schlenker-Roberts damage function via nonlinear least squares.

        y(T) = y_max * exp(-((T - T_opt) / sigma)^2)
        """
        try:
            p0 = [np.max(yields), float(temps[np.argmax(yields)]), 5.0]
            bounds = ([0, temps.min() - 10, 0.1], [np.max(yields) * 3, temps.max() + 10, 50.0])
            popt, _ = optimize.curve_fit(
                lambda T, ym, To, s: ym * np.exp(-((T - To) / s) ** 2),
                temps, yields, p0=p0, bounds=bounds, maxfev=5000,
            )
            return tuple(popt)
        except (RuntimeError, ValueError):
            return None
