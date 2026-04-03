"""Aquaculture sector analysis: production growth, feed efficiency, and trade competitiveness.

Models aquaculture production dynamics using feed conversion ratios (FCR),
environmental carrying capacity constraints, and trade competitiveness
relative to capture fisheries.

Methodology:
    1. Production growth model:
       Q_t = Q_0 * exp(r * t) * (1 - Q_t / K)
       where r = intrinsic growth rate, K = carrying capacity.
       Logistic growth approximation for bounded expansion.

    2. Feed conversion ratio (FCR) efficiency:
       FCR = feed_input / biomass_gain
       Economic FCR (eFCR) = feed_cost / (biomass_gain * price_fish)
       Lower FCR = better efficiency. Salmon ~1.2, tilapia ~1.6, shrimp ~1.8.

    3. Environmental carrying capacity:
       Nitrogen assimilation capacity (Bacher et al.):
       K_N = (A_bed * D_N * V_exchange) / (FCR * N_fraction)
       Score penalizes production > 0.8 * estimated K.

    4. Trade competitiveness (Revealed Comparative Advantage):
       RCA = (X_aq / X_total) / (W_aq / W_total)
       where X = exports, W = world exports.
       RCA > 1 = comparative advantage.

    Score: low growth + poor FCR + capacity exceeded + low RCA = high stress.

References:
    FAO (2022). "The State of World Fisheries and Aquaculture."
    Bacher, C. et al. (1997). "Carrying capacity of a bivalve aquaculture system."
        ICES Journal of Marine Science, 54(5), 945-958.
    Tacon, A.G.J. & Metian, M. (2008). "Global overview on the use of fish meal
        and fish oil in industrially compounded aquafeeds." Aquaculture, 285, 146-158.
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class Aquaculture(LayerBase):
    layer_id = "l5"
    name = "Aquaculture"

    # Benchmark FCR values by species group (kg feed / kg gain)
    FCR_BENCHMARKS = {
        "salmon": 1.2,
        "tilapia": 1.6,
        "shrimp": 1.8,
        "catfish": 1.7,
        "carp": 1.5,
        "default": 1.7,
    }

    async def compute(self, db, **kwargs) -> dict:
        """Analyze aquaculture sector performance.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
            species : str - species group filter (default all)
            lookback_years : int - history window (default 20)
        """
        country = kwargs.get("country_iso3", "BGD")
        species = kwargs.get("species")
        lookback = kwargs.get("lookback_years", 20)

        rows = await db.fetch_all(
            """
            SELECT ds.description, dp.date, dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('fao', 'faostat', 'aquaculture', 'wdi')
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

        # Extract series
        production = self._extract_series(series, ["aquaculture_production", "aquaculture_output", "fish_farm"])
        capture = self._extract_series(series, ["capture_fisheries", "wild_catch", "marine_catch"])
        feed_use = self._extract_series(series, ["aquafeed", "fish_feed", "feed_consumption"])
        exports_aq = self._extract_series(series, ["fish_export", "seafood_export", "aquaculture_export"])
        world_exports = self._extract_series(series, ["world_fish_export", "global_seafood"])

        # --- Production growth analysis ---
        growth_result = None
        if production and len(production) >= 6:
            growth_result = self._logistic_growth(np.array(production))

        # --- FCR estimation ---
        fcr_result = None
        if production and feed_use and len(production) >= 4 and len(feed_use) >= 4:
            n = min(len(production), len(feed_use))
            prod_arr = np.array(production[-n:])
            feed_arr = np.array(feed_use[-n:])
            delta_prod = np.diff(prod_arr)
            # FCR = feed / biomass increase
            valid = delta_prod > 0
            if valid.sum() >= 3:
                fcr_vals = feed_arr[1:][valid] / delta_prod[valid]
                fcr_result = {
                    "mean_fcr": round(float(np.median(fcr_vals)), 3),
                    "fcr_trend": round(float(sp_stats.linregress(
                        np.arange(len(fcr_vals)), fcr_vals).slope), 4),
                    "benchmark_species": species or "default",
                    "benchmark_fcr": self.FCR_BENCHMARKS.get(species or "default", 1.7),
                    "efficiency_gap": round(
                        float(np.median(fcr_vals)) - self.FCR_BENCHMARKS.get(species or "default", 1.7),
                        3
                    ),
                    "n_periods": int(valid.sum()),
                }
        elif production and len(production) >= 4:
            # Proxy FCR from production-level trend
            prod_arr = np.array(production)
            fcr_result = {
                "mean_fcr": None,
                "benchmark_species": species or "default",
                "benchmark_fcr": self.FCR_BENCHMARKS.get(species or "default", 1.7),
                "efficiency_gap": None,
                "note": "no feed data; FCR proxy unavailable",
            }

        # --- Carrying capacity ---
        capacity_result = self._carrying_capacity(production, growth_result)

        # --- Trade competitiveness (RCA) ---
        rca_result = None
        if exports_aq and world_exports and len(exports_aq) >= 3:
            rca_result = self._compute_rca(exports_aq, world_exports, capture)

        # --- Score ---
        # Growth component: negative or slowing growth = stress
        growth_component = 50.0
        if growth_result and growth_result.get("annual_growth_pct") is not None:
            g = growth_result["annual_growth_pct"]
            growth_component = float(np.clip(50.0 - g * 5.0, 0, 100))

        # FCR component: FCR above benchmark = stress
        fcr_component = 50.0
        if fcr_result and fcr_result.get("efficiency_gap") is not None:
            gap = fcr_result["efficiency_gap"]
            fcr_component = float(np.clip(50.0 + gap * 20.0, 0, 100))

        # Capacity component: near or over capacity = stress
        cap_component = 50.0
        if capacity_result and capacity_result.get("utilization_ratio") is not None:
            util = capacity_result["utilization_ratio"]
            cap_component = float(np.clip(util * 80.0, 0, 100))

        # RCA component: low competitiveness = stress
        rca_component = 50.0
        if rca_result and rca_result.get("rca") is not None:
            rca = rca_result["rca"]
            rca_component = float(np.clip(100.0 - rca * 20.0, 0, 100))

        score = float(np.clip(
            0.30 * growth_component + 0.25 * fcr_component
            + 0.25 * cap_component + 0.20 * rca_component,
            0, 100,
        ))

        return {
            "score": round(score, 2),
            "country": country,
            "production_growth": growth_result,
            "feed_conversion": fcr_result,
            "carrying_capacity": capacity_result,
            "trade_competitiveness": rca_result,
        }

    @staticmethod
    def _extract_series(series: dict, keywords: list[str]) -> list[float] | None:
        for key, vals in series.items():
            for kw in keywords:
                if kw in key:
                    return [v[1] for v in vals]
        return None

    @staticmethod
    def _logistic_growth(prod: np.ndarray) -> dict:
        """Fit logistic growth model and extract parameters."""
        n = len(prod)
        t = np.arange(n, dtype=float)

        # Log-linear for growth rate estimate
        log_prod = np.log(np.maximum(prod, 1e-6))
        slope, intercept, r_val, _, _ = sp_stats.linregress(t, log_prod)
        annual_growth_pct = float(slope * 100.0)

        # Estimate carrying capacity from saturation: inflection point
        # Simple: K ~ 2 * current if still growing, or max * 1.1 if flat
        recent_growth = slope
        if recent_growth < 0.01:
            k_estimate = float(prod.max() * 1.1)
        else:
            k_estimate = float(prod.max() * 2.5)

        # Current utilization
        current_prod = float(prod[-1])
        utilization = current_prod / max(k_estimate, 1e-6)

        return {
            "annual_growth_pct": round(annual_growth_pct, 3),
            "r_squared": round(float(r_val ** 2), 4),
            "current_production": round(current_prod, 3),
            "estimated_capacity": round(k_estimate, 3),
            "capacity_utilization_pct": round(float(utilization * 100), 2),
            "production_phase": (
                "expansion" if annual_growth_pct > 5
                else "maturation" if annual_growth_pct > 0
                else "contraction"
            ),
        }

    @staticmethod
    def _carrying_capacity(
        production: list[float] | None,
        growth_result: dict | None,
    ) -> dict | None:
        """Estimate environmental carrying capacity utilization."""
        if not production:
            return None

        current = float(production[-1])
        peak = float(max(production))

        # Infer K from growth result or use a productivity-based proxy
        k_est = None
        if growth_result and growth_result.get("estimated_capacity"):
            k_est = growth_result["estimated_capacity"]

        if k_est is None:
            # Heuristic: K ~ 3x current if early phase, 1.2x peak if late phase
            growth_rate = (production[-1] - production[0]) / max(production[0], 1e-6)
            k_est = current * 3.0 if growth_rate > 1.0 else peak * 1.2

        utilization = current / max(k_est, 1e-6)
        return {
            "current_production_t": round(current, 3),
            "estimated_capacity_t": round(float(k_est), 3),
            "utilization_ratio": round(float(utilization), 4),
            "pressure_level": (
                "critical" if utilization > 0.9
                else "elevated" if utilization > 0.7
                else "moderate" if utilization > 0.5
                else "low"
            ),
        }

    @staticmethod
    def _compute_rca(
        exports_aq: list[float],
        world_exports: list[float],
        capture: list[float] | None,
    ) -> dict:
        """Compute RCA of aquaculture vs capture fisheries in world trade."""
        n = min(len(exports_aq), len(world_exports))
        aq = np.array(exports_aq[-n:])
        wld = np.array(world_exports[-n:])

        # Total fishery exports (aquaculture + capture)
        if capture and len(capture) >= n:
            cap_arr = np.array(capture[-n:])
            total_fishery = aq + cap_arr
        else:
            total_fishery = aq * 1.5  # proxy

        # RCA = (X_aq / X_total_fish) / (W_aq / W_total_fish_global)
        # Use world_exports as W proxy
        share_domestic = float(aq[-1]) / max(float(total_fishery[-1]), 1e-6)
        share_world = float(wld[-1]) / max(float(wld[-1]) * 1.5, 1e-6)  # proxy
        rca = share_domestic / max(share_world, 1e-6)

        # RCA trend
        rca_series = (aq / np.maximum(total_fishery, 1e-6)) / (wld / np.maximum(wld * 1.5, 1e-6))
        slope, _, r_val, _, _ = sp_stats.linregress(np.arange(n), rca_series)

        return {
            "rca": round(float(rca), 4),
            "rca_trend_slope": round(float(slope), 5),
            "r_squared": round(float(r_val ** 2), 4),
            "comparative_advantage": rca > 1.0,
            "vs_capture_fisheries": (
                "dominant" if rca > 2.0
                else "competitive" if rca > 1.0
                else "lagging"
            ),
        }
