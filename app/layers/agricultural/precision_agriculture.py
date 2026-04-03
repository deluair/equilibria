"""Precision agriculture technology adoption and yield gain analysis.

Models yield improvements from precision agriculture (PA) technologies
using variable rate application (VRA) ROI, technology diffusion curves,
and data-driven farming adoption rates.

Methodology:
    1. Yield gain from PA technologies:
       Delta_Y = sum_i w_i * gamma_i * A_i
       where gamma_i = yield response coefficient for technology i,
       A_i = adoption rate, w_i = acreage weight.

    2. Variable rate application (VRA) ROI:
       ROI_VRA = (Delta_Y * P_crop - Delta_C_input) / C_VRA
       where Delta_C_input = input cost savings from precision application.

    3. Technology diffusion (Bass model):
       dA/dt = (p + q * A(t)) * (1 - A(t))
       where p = innovation coefficient, q = imitation coefficient.
       Cumulative adoption: A(t) = (1 - exp(-(p+q)*t)) / (1 + (q/p)*exp(-(p+q)*t))

    Score: low adoption + low ROI + low diffusion velocity = high vulnerability
    (agricultural sector falling behind precision-technology frontier).

References:
    Mulla, D.J. (2013). "Twenty five years of remote sensing in precision
        agriculture." Biosystems Engineering, 114(4), 358-371.
    Bass, F.M. (1969). "A New Product Growth for Model Consumer Durables."
        Management Science, 15(5), 215-227.
    Schimmelpfennig, D. (2016). "Farm Profits and Adoption of Precision
        Agriculture." USDA-ERS Economic Research Report No. 217.
"""

from __future__ import annotations

import numpy as np
from scipy import optimize
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class PrecisionAgriculture(LayerBase):
    layer_id = "l5"
    name = "Precision Agriculture"

    # PA technology categories and typical yield gain ranges (%)
    TECH_YIELD_GAINS = {
        "gps_guidance": (1.0, 3.0),
        "variable_rate_application": (3.0, 8.0),
        "remote_sensing": (2.0, 5.0),
        "soil_mapping": (2.0, 6.0),
        "yield_monitoring": (1.0, 3.0),
        "decision_support_systems": (2.0, 7.0),
    }

    # Bass model typical parameters for agricultural technology
    BASS_P_DEFAULT = 0.003   # innovation coefficient
    BASS_Q_DEFAULT = 0.15    # imitation coefficient

    async def compute(self, db, **kwargs) -> dict:
        """Estimate PA technology adoption effects and yield gains.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
            crop : str - specific crop (default all major crops)
            lookback_years : int - adoption history (default 15)
        """
        country = kwargs.get("country_iso3", "BGD")
        crop = kwargs.get("crop")
        lookback = kwargs.get("lookback_years", 15)

        rows = await db.fetch_all(
            """
            SELECT ds.description, dp.date, dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('fao', 'wdi', 'usda', 'precision_ag')
              AND ds.country_iso3 = ?
              AND dp.date >= date('now', ?)
            ORDER BY ds.description, dp.date
            """,
            (country, f"-{lookback} years"),
        )

        series: dict[str, list[tuple[str, float]]] = {}
        for r in rows:
            desc = (r["description"] or "").lower()
            series.setdefault(desc, []).append((r["date"], float(r["value"])))

        # Extract relevant series
        yield_series = self._extract_series(series, ["yield", "crop_yield", "cereal_yield"])
        adoption_series = self._extract_series(series, ["precision_ag", "pa_adoption", "technology_adoption"])
        vra_series = self._extract_series(series, ["variable_rate", "vra", "input_use_efficiency"])
        farm_income = self._extract_series(series, ["farm_income", "agricultural_income", "farm_profit"])

        # --- Technology diffusion (Bass model) ---
        diffusion_result = None
        if adoption_series and len(adoption_series) >= 5:
            diffusion_result = self._fit_bass_model(np.array(adoption_series))

        # If no adoption data, estimate from yield trend inflection
        elif yield_series and len(yield_series) >= 8:
            diffusion_result = self._infer_diffusion_from_yield(np.array(yield_series))

        # --- VRA ROI estimation ---
        vra_roi = self._estimate_vra_roi(vra_series, yield_series, farm_income)

        # --- Technology gap scoring ---
        tech_gap_score = self._compute_tech_gap(adoption_series, diffusion_result)

        # --- Yield gain decomposition ---
        yield_gain = self._decompose_yield_gain(yield_series)

        # --- Data-driven farming index ---
        ddf_index = self._data_driven_farming_index(series)

        # Score: high adoption + good ROI + rapid diffusion = low stress (low score)
        # Low adoption + poor ROI = high stress
        adoption_score = 100.0 - tech_gap_score  # invert: gap drives score up

        roi_component = 50.0
        if vra_roi is not None and vra_roi.get("roi") is not None:
            roi_val = vra_roi["roi"]
            # Negative ROI -> high stress (100), ROI > 2.0 -> low stress (0)
            roi_component = float(np.clip(100.0 - roi_val * 25.0, 0, 100))

        diffusion_component = 50.0
        if diffusion_result and diffusion_result.get("current_adoption") is not None:
            adopt = diffusion_result["current_adoption"]
            # 0% adoption = 100 score, 80%+ adoption = 0 score
            diffusion_component = float(np.clip(100.0 - adopt * 1.25, 0, 100))

        score = float(np.clip(
            0.35 * adoption_score + 0.30 * roi_component + 0.35 * diffusion_component,
            0, 100,
        ))

        return {
            "score": round(score, 2),
            "country": country,
            "technology_diffusion": diffusion_result,
            "vra_roi": vra_roi,
            "yield_gain_decomposition": yield_gain,
            "data_driven_farming_index": ddf_index,
            "tech_gap_score": round(tech_gap_score, 2),
        }

    @staticmethod
    def _extract_series(series: dict, keywords: list[str]) -> list[float] | None:
        for key, vals in series.items():
            for kw in keywords:
                if kw in key:
                    return [v[1] for v in vals]
        return None

    def _fit_bass_model(self, adoption: np.ndarray) -> dict:
        """Fit Bass diffusion model to cumulative adoption data.

        F(t) = (1 - exp(-(p+q)*t)) / (1 + (q/p)*exp(-(p+q)*t))
        """
        n = len(adoption)
        t = np.arange(n, dtype=float)
        # Normalize adoption to [0,1] range
        adopt_norm = adoption / max(adoption.max(), 1e-6)
        adopt_norm = np.clip(adopt_norm, 0.001, 0.999)

        def bass(t_arr, p, q):
            return (1 - np.exp(-(p + q) * t_arr)) / (1 + (q / max(p, 1e-9)) * np.exp(-(p + q) * t_arr))

        try:
            popt, _ = optimize.curve_fit(
                bass, t, adopt_norm,
                p0=[self.BASS_P_DEFAULT, self.BASS_Q_DEFAULT],
                bounds=([1e-5, 1e-5], [0.5, 1.0]),
                maxfev=5000,
            )
            p, q = popt
            fitted = bass(t, p, q)
            ss_res = float(np.sum((adopt_norm - fitted) ** 2))
            ss_tot = float(np.sum((adopt_norm - adopt_norm.mean()) ** 2))
            r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

            # Forecast 5 years ahead
            t_future = np.arange(n, n + 5, dtype=float)
            future_adoption = bass(t_future, p, q) * 100.0

            peak_time = np.log(q / p) / (p + q) if p > 0 and q > 0 else None

            return {
                "bass_p": round(float(p), 5),
                "bass_q": round(float(q), 5),
                "r_squared": round(r2, 4),
                "current_adoption": round(float(adopt_norm[-1] * 100), 2),
                "peak_growth_year": round(float(peak_time), 1) if peak_time else None,
                "adoption_5yr_forecast_pct": round(float(future_adoption[-1]), 2),
                "diffusion_speed": "fast" if q > 0.20 else "moderate" if q > 0.10 else "slow",
            }
        except (RuntimeError, ValueError):
            return {
                "current_adoption": round(float(adopt_norm[-1] * 100), 2),
                "diffusion_speed": "unknown",
                "note": "Bass model fit failed; raw adoption reported",
            }

    @staticmethod
    def _infer_diffusion_from_yield(yields: np.ndarray) -> dict:
        """Infer technology adoption from yield trend acceleration."""
        n = len(yields)
        t = np.arange(n, dtype=float)
        # Fit linear trend to detect structural acceleration
        slope, intercept, r_val, _, se = sp_stats.linregress(t, yields)
        # Recent 5-year vs earlier slope
        if n >= 10:
            slope_recent, *_ = sp_stats.linregress(t[-5:], yields[-5:])
            slope_early, *_ = sp_stats.linregress(t[:-5], yields[:-5])
            acceleration = slope_recent - slope_early
        else:
            acceleration = 0.0

        return {
            "yield_trend_slope": round(float(slope), 4),
            "yield_r_squared": round(float(r_val ** 2), 4),
            "trend_acceleration": round(float(acceleration), 4),
            "inferred_from": "yield_trend",
            "diffusion_speed": "fast" if acceleration > 0.5 else "moderate" if acceleration > 0 else "slow",
        }

    @staticmethod
    def _estimate_vra_roi(
        vra_series: list[float] | None,
        yield_series: list[float] | None,
        income_series: list[float] | None,
    ) -> dict | None:
        """Estimate variable rate application ROI from input efficiency and yield."""
        if not yield_series or len(yield_series) < 4:
            return None

        y = np.array(yield_series)
        yield_trend_slope, _, _, _, _ = sp_stats.linregress(np.arange(len(y)), y)

        # Proxy for VRA input savings: if fertilizer/input use per ton yield declining
        input_efficiency = None
        if vra_series and len(vra_series) >= 4:
            v = np.array(vra_series)
            eff_slope, _, _, _, _ = sp_stats.linregress(np.arange(len(v)), v)
            input_efficiency = float(eff_slope)

        # Simplified ROI: yield gain value / PA investment cost
        # Yield gain rate per year * price proxy
        yield_gain_annual = float(yield_trend_slope) / max(float(np.mean(y)), 1e-6) * 100.0

        # Typical PA cost is 5-10% of farm income; ROI = yield_gain / cost
        pa_cost_fraction = 0.07
        roi = yield_gain_annual / (pa_cost_fraction * 100.0) if yield_gain_annual > 0 else -1.0

        return {
            "yield_gain_rate_pct_yr": round(yield_gain_annual, 4),
            "input_efficiency_trend": round(input_efficiency, 4) if input_efficiency is not None else None,
            "roi": round(float(roi), 3),
            "roi_interpretation": "positive" if roi > 0 else "negative",
        }

    @staticmethod
    def _compute_tech_gap(
        adoption_series: list[float] | None,
        diffusion_result: dict | None,
    ) -> float:
        """Tech gap as distance from technological frontier (100% adoption)."""
        current = 0.0
        if adoption_series:
            current = max(float(adoption_series[-1]), 0.0)
            if current > 1.0:
                current = min(current, 100.0)
            else:
                current = current * 100.0
        elif diffusion_result and diffusion_result.get("current_adoption") is not None:
            current = float(diffusion_result["current_adoption"])

        return float(np.clip(100.0 - current, 0, 100))

    @staticmethod
    def _decompose_yield_gain(yield_series: list[float] | None) -> dict | None:
        """Decompose yield growth into trend, weather noise, and technology."""
        if not yield_series or len(yield_series) < 8:
            return None

        y = np.array(yield_series)
        n = len(y)
        t = np.arange(n, dtype=float)

        # Linear trend (technology proxy)
        slope, intercept, r_val, _, _ = sp_stats.linregress(t, y)
        trend_contrib = slope * n / max(float(y[0]), 1e-6) * 100.0

        # Residual variance (weather noise)
        fitted = slope * t + intercept
        resid = y - fitted
        cv_residual = float(np.std(resid) / np.mean(y) * 100.0) if np.mean(y) > 0 else 0.0

        return {
            "technology_trend_pct_total": round(float(trend_contrib), 2),
            "weather_noise_cv_pct": round(cv_residual, 2),
            "yield_r_squared": round(float(r_val ** 2), 4),
            "n_years": n,
            "mean_yield": round(float(np.mean(y)), 3),
        }

    @staticmethod
    def _data_driven_farming_index(series: dict) -> dict:
        """Composite data-driven farming index from available proxies."""
        proxies = {
            "digital_infra": ["internet_access", "mobile_penetration", "broadband"],
            "input_modernization": ["fertilizer_kg_ha", "pesticide_use", "tractor_density"],
            "market_access": ["market_access", "road_density", "storage_capacity"],
        }
        scores = {}
        for dim, keywords in proxies.items():
            for key, vals in series.items():
                for kw in keywords:
                    if kw in key and vals:
                        scores[dim] = float(vals[-1][1])
                        break
                if dim in scores:
                    break

        if not scores:
            return {"index": None, "n_dimensions": 0}

        return {
            "index": round(float(np.mean(list(scores.values()))), 3),
            "n_dimensions": len(scores),
            "dimensions": {k: round(v, 3) for k, v in scores.items()},
        }
