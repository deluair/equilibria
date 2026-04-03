"""Environmental Kuznets Curve: CO2-income inverted-U estimation.

Estimates the Environmental Kuznets Curve (EKC) hypothesis that pollution
first rises then falls with economic development. Uses panel fixed effects
with country heterogeneity to estimate turning points by pollutant type.

Methodology:
    Panel FE specification:

        ln(E_it/P_it) = alpha_i + b1*ln(y_it) + b2*[ln(y_it)]^2
                         + b3*[ln(y_it)]^3 + gamma*Z_it + e_it

    where E is emissions, P is population, y is GDP per capita, alpha_i
    are country fixed effects, Z are controls (trade openness, industry
    share, urbanization).

    Turning point: dy/dx = b1 + 2*b2*ln(y) + 3*b3*[ln(y)]^2 = 0.

    The cubic term tests for N-shaped EKC (pollution re-rising at very
    high income levels).

References:
    Grossman, G.M. & Krueger, A.B. (1995). "Economic growth and the
        environment." Quarterly Journal of Economics, 110(2), 353-377.
    Stern, D.I. (2004). "The rise and fall of the Environmental Kuznets Curve."
        World Development, 32(8), 1419-1439.
    Dasgupta, S. et al. (2002). "Confronting the Environmental Kuznets Curve."
        Journal of Economic Perspectives, 16(1), 147-168.
"""

from __future__ import annotations

import numpy as np
from scipy import optimize

from app.layers.base import LayerBase


class EnvironmentalKuznetsCurve(LayerBase):
    layer_id = "l9"
    name = "Environmental Kuznets Curve"

    # Pollutants to test
    POLLUTANT_SERIES = {
        "co2_pc": "EN.ATM.CO2E.PC",
        "pm25": "EN.ATM.PM25.MC.M3",
        "methane_pc": "EN.ATM.METH.PC",
        "nox_pc": "EN.ATM.NOXE.PC",
    }

    async def compute(self, db, **kwargs) -> dict:
        """Estimate Environmental Kuznets Curve.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - focus country (default None = all)
            pollutant : str - which pollutant (default "co2_pc")
            include_cubic : bool - test N-shaped EKC (default True)
        """
        country = kwargs.get("country_iso3")
        pollutant = kwargs.get("pollutant", "co2_pc")
        include_cubic = kwargs.get("include_cubic", True)

        series_id = self.POLLUTANT_SERIES.get(pollutant, "EN.ATM.CO2E.PC")

        # Fetch pollutant data (cross-country panel)
        pollutant_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = ?
              AND dp.value > 0
            ORDER BY ds.country_iso3, dp.date
            """,
            (series_id,),
        )

        # Fetch GDP per capita
        gdp_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.PCAP.KD'
              AND dp.value > 0
            ORDER BY ds.country_iso3, dp.date
            """,
        )

        if not pollutant_rows or not gdp_rows:
            return {"score": 50, "error": "no pollutant or GDP data"}

        # Build matched panel
        poll_data: dict[str, dict[str, float]] = {}
        for r in pollutant_rows:
            yr = r["date"][:4] if isinstance(r["date"], str) else str(r["date"])
            poll_data.setdefault(r["country_iso3"], {})[yr] = float(r["value"])

        gdp_data: dict[str, dict[str, float]] = {}
        for r in gdp_rows:
            yr = r["date"][:4] if isinstance(r["date"], str) else str(r["date"])
            gdp_data.setdefault(r["country_iso3"], {})[yr] = float(r["value"])

        # Build estimation sample
        y_list, x_list, x2_list, x3_list = [], [], [], []
        country_ids = []
        country_map: dict[str, int] = {}
        counter = 0

        for iso in set(poll_data.keys()) & set(gdp_data.keys()):
            common = sorted(set(poll_data[iso].keys()) & set(gdp_data[iso].keys()))
            for yr in common:
                e_val = poll_data[iso][yr]
                g_val = gdp_data[iso][yr]
                if e_val > 0 and g_val > 0:
                    log_e = np.log(e_val)
                    log_g = np.log(g_val)
                    y_list.append(log_e)
                    x_list.append(log_g)
                    x2_list.append(log_g ** 2)
                    x3_list.append(log_g ** 3)
                    if iso not in country_map:
                        country_map[iso] = counter
                        counter += 1
                    country_ids.append(country_map[iso])

        n = len(y_list)
        if n < 30:
            return {"score": 50, "error": "insufficient panel observations"}

        y = np.array(y_list)
        log_gdp = np.array(x_list)
        log_gdp_sq = np.array(x2_list)
        log_gdp_cu = np.array(x3_list)
        groups = np.array(country_ids)

        # Within-group demeaning (fixed effects)
        n_countries = len(country_map)
        arrays_to_demean = [y, log_gdp, log_gdp_sq]
        if include_cubic:
            arrays_to_demean.append(log_gdp_cu)
        demeaned = self._demean(*arrays_to_demean, groups=groups)

        y_dm = demeaned[0]
        x1_dm = demeaned[1]
        x2_dm = demeaned[2]

        # Quadratic specification
        X_quad = np.column_stack([x1_dm, x2_dm])
        beta_quad = np.linalg.lstsq(X_quad, y_dm, rcond=None)[0]
        fitted_quad = X_quad @ beta_quad
        resid_quad = y_dm - fitted_quad
        ss_res_q = float(np.sum(resid_quad ** 2))
        ss_tot = float(np.sum((y_dm - y_dm.mean()) ** 2))
        r2_quad = 1.0 - ss_res_q / ss_tot if ss_tot > 0 else 0.0

        # HC1 standard errors for quadratic
        se_quad = self._hc1_se(X_quad, resid_quad, n, n_countries)

        # Quadratic turning point
        b1_q, b2_q = float(beta_quad[0]), float(beta_quad[1])
        turning_point_quad = None
        inverted_u_quad = b1_q > 0 and b2_q < 0
        if b2_q != 0:
            tp_log = -b1_q / (2 * b2_q)
            turning_point_quad = {
                "log_gdp_pc": round(tp_log, 4),
                "gdp_pc_usd": round(float(np.exp(tp_log)), 0),
                "in_sample": float(np.min(log_gdp)) <= tp_log <= float(np.max(log_gdp)),
            }

        # Cubic specification (N-shaped test)
        cubic_result = None
        if include_cubic:
            x3_dm = demeaned[3]
            X_cub = np.column_stack([x1_dm, x2_dm, x3_dm])
            beta_cub = np.linalg.lstsq(X_cub, y_dm, rcond=None)[0]
            fitted_cub = X_cub @ beta_cub
            resid_cub = y_dm - fitted_cub
            ss_res_c = float(np.sum(resid_cub ** 2))
            r2_cub = 1.0 - ss_res_c / ss_tot if ss_tot > 0 else 0.0
            se_cub = self._hc1_se(X_cub, resid_cub, n, n_countries)

            b1_c = float(beta_cub[0])
            b2_c = float(beta_cub[1])
            b3_c = float(beta_cub[2])

            # Turning points from cubic: solve 3*b3*x^2 + 2*b2*x + b1 = 0
            disc = (2 * b2_c) ** 2 - 4 * (3 * b3_c) * b1_c
            turning_points_cubic = []
            if disc >= 0 and b3_c != 0:
                sqrt_disc = np.sqrt(disc)
                tp1 = (-2 * b2_c + sqrt_disc) / (2 * 3 * b3_c)
                tp2 = (-2 * b2_c - sqrt_disc) / (2 * 3 * b3_c)
                for tp in sorted([tp1, tp2]):
                    turning_points_cubic.append({
                        "log_gdp_pc": round(tp, 4),
                        "gdp_pc_usd": round(float(np.exp(tp)), 0),
                        "in_sample": float(np.min(log_gdp)) <= tp <= float(np.max(log_gdp)),
                    })

            n_shaped = b3_c > 0 and abs(b3_c / float(se_cub[2])) > 1.96 if se_cub[2] > 0 else False

            cubic_result = {
                "beta_log_gdp": round(b1_c, 6),
                "beta_log_gdp_sq": round(b2_c, 6),
                "beta_log_gdp_cu": round(b3_c, 6),
                "se_log_gdp": round(float(se_cub[0]), 6),
                "se_log_gdp_sq": round(float(se_cub[1]), 6),
                "se_log_gdp_cu": round(float(se_cub[2]), 6),
                "r2_within": round(r2_cub, 4),
                "n_shaped": n_shaped,
                "turning_points": turning_points_cubic,
            }

        # Target country analysis
        target_analysis = None
        if country and country in poll_data and country in gdp_data:
            latest_years = sorted(set(poll_data[country].keys()) & set(gdp_data[country].keys()))
            if latest_years:
                latest_yr = latest_years[-1]
                latest_e = poll_data[country][latest_yr]
                latest_g = gdp_data[country][latest_yr]
                log_g_target = np.log(latest_g)

                if turning_point_quad and inverted_u_quad:
                    tp = turning_point_quad["log_gdp_pc"]
                    phase = "pre_peak" if log_g_target < tp else "post_peak"
                    distance_to_tp = log_g_target - tp
                else:
                    phase = "no_inverted_u"
                    distance_to_tp = None

                target_analysis = {
                    "country_iso3": country,
                    "latest_year": latest_yr,
                    "emissions_pc": round(latest_e, 4),
                    "gdp_pc": round(latest_g, 2),
                    "log_gdp_pc": round(float(log_g_target), 4),
                    "phase": phase,
                    "distance_to_turning_point": round(float(distance_to_tp), 4) if distance_to_tp is not None else None,
                }

        # Score: pre-peak + high emissions growth = high stress
        if target_analysis and target_analysis["phase"] == "pre_peak":
            # Distance below turning point drives score
            dist_below = abs(target_analysis.get("distance_to_turning_point", 0) or 0)
            score = float(np.clip(50 + dist_below * 15, 30, 85))
        elif target_analysis and target_analysis["phase"] == "post_peak":
            score = float(np.clip(30 - (target_analysis.get("distance_to_turning_point", 0) or 0) * 5, 10, 45))
        else:
            # No target country or no inverted U
            score = 50.0 if inverted_u_quad else 60.0

        return {
            "score": round(score, 2),
            "pollutant": pollutant,
            "n_obs": n,
            "n_countries": n_countries,
            "quadratic_fe": {
                "beta_log_gdp": round(b1_q, 6),
                "beta_log_gdp_sq": round(b2_q, 6),
                "se_log_gdp": round(float(se_quad[0]), 6),
                "se_log_gdp_sq": round(float(se_quad[1]), 6),
                "r2_within": round(r2_quad, 4),
                "inverted_u": inverted_u_quad,
                "turning_point": turning_point_quad,
            },
            "cubic_fe": cubic_result,
            "target": target_analysis,
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
    def _hc1_se(X: np.ndarray, resid: np.ndarray, n: int, n_groups: int) -> np.ndarray:
        """HC1 robust standard errors adjusted for FE degrees of freedom."""
        k = X.shape[1]
        dof = n - k - n_groups
        XtX_inv = np.linalg.pinv(X.T @ X)
        scale = n / max(dof, 1)
        omega = np.diag(resid ** 2) * scale
        V = XtX_inv @ (X.T @ omega @ X) @ XtX_inv
        return np.sqrt(np.maximum(np.diag(V), 0.0))
