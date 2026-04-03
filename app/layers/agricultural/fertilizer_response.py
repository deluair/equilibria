"""Yield response to fertilizer application.

Models the agronomic relationship between fertilizer input and crop yield
using standard response functions, then computes economic optimum application
rates via marginal value product analysis.

Methodology:
    Three response functions are estimated:

    1. Quadratic:  y = a + b*N + c*N^2  (c < 0 for diminishing returns)
    2. Mitscherlich: y = A * (1 - exp(-c * (N + b)))
       where A = maximum attainable yield, c = curvature, b = soil N equiv.
    3. Linear-plateau: y = min(a + b*N, y_plateau)

    Economic optimum N rate (EONR) satisfies:
        dy/dN = price_fertilizer / price_crop  (= price ratio, PR)

    For the quadratic: N* = (PR - b) / (2 * c)
    Marginal value product (MVP) = price_crop * dy/dN
    Value-cost ratio (VCR) = MVP / price_fertilizer at current N

    Score reflects departure from optimal fertilizer use:
    overapplication or severe underapplication both raise the score.

References:
    Mitscherlich, E.A. (1909). "Das Gesetz des Minimums und das Gesetz
        des abnehmenden Bodenertrages." Landw. Jahrb., 38, 537-552.
    Cerrato, M.E. & Blackmer, A.M. (1990). "Comparison of models for
        describing corn yield response to nitrogen fertilizer."
        Agronomy Journal, 82(1), 138-143.
    Morris, M., Kelly, V., Kopicki, R. & Byerlee, D. (2007).
        "Fertilizer Use in African Agriculture." World Bank.
"""

from __future__ import annotations

import numpy as np
from scipy import optimize

from app.layers.base import LayerBase


class FertilizerResponse(LayerBase):
    layer_id = "l5"
    name = "Fertilizer Response"

    async def compute(self, db, **kwargs) -> dict:
        """Estimate fertilizer response curves and compute optimal rates.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
            crop : str - crop filter (default all)
            fertilizer_price : float - price per kg N (default from data)
            crop_price : float - price per kg crop output (default from data)
        """
        country = kwargs.get("country_iso3", "BGD")
        crop = kwargs.get("crop")
        fert_price = kwargs.get("fertilizer_price")
        crop_price = kwargs.get("crop_price")

        crop_clause = "AND ds.description LIKE '%' || ? || '%'" if crop else ""
        params = [country]
        if crop:
            params.append(crop)

        rows = await db.fetch_all(
            f"""
            SELECT dp.value AS yield_val, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source = 'fertilizer_trials'
              AND ds.country_iso3 = ?
              {crop_clause}
            ORDER BY dp.date
            """,
            tuple(params),
        )

        if not rows or len(rows) < 8:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient fertilizer trial data"}

        import json

        n_rates = []
        yields = []
        prices_fert = []
        prices_crop = []

        for row in rows:
            y_val = row["yield_val"]
            if y_val is None or y_val <= 0:
                continue
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            n_rate = meta.get("nitrogen_kg_ha")
            if n_rate is None:
                continue
            n_rates.append(float(n_rate))
            yields.append(float(y_val))
            if meta.get("fertilizer_price_per_kg"):
                prices_fert.append(float(meta["fertilizer_price_per_kg"]))
            if meta.get("crop_price_per_kg"):
                prices_crop.append(float(meta["crop_price_per_kg"]))

        n = len(n_rates)
        if n < 8:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient valid observations"}

        N = np.array(n_rates)
        Y = np.array(yields)

        # Use provided prices or median from data
        pf = fert_price if fert_price else (float(np.median(prices_fert)) if prices_fert else 1.0)
        pc = crop_price if crop_price else (float(np.median(prices_crop)) if prices_crop else 0.3)
        price_ratio = pf / pc if pc > 0 else float("inf")

        # 1. Quadratic response
        quad_result = self._fit_quadratic(N, Y, price_ratio)

        # 2. Mitscherlich response
        mits_result = self._fit_mitscherlich(N, Y, price_ratio)

        # 3. Linear-plateau
        lp_result = self._fit_linear_plateau(N, Y, price_ratio)

        # Pick best-fitting model by R-squared
        models = {}
        if quad_result:
            models["quadratic"] = quad_result
        if mits_result:
            models["mitscherlich"] = mits_result
        if lp_result:
            models["linear_plateau"] = lp_result

        if not models:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "all response function fits failed"}

        best_name = max(models, key=lambda k: models[k].get("r_squared", 0))
        best = models[best_name]
        eonr = best.get("eonr")

        # Current average application rate
        current_n = float(N.mean())

        # Marginal value product at current rate
        mvp_current = best.get("mvp_at_current")

        # Value-cost ratio at current rate
        vcr = (mvp_current * pc) / pf if mvp_current and pf > 0 else None

        # Score: deviation from economic optimum
        # Perfect alignment = 0, over/under by 50+ kg/ha = 100
        if eonr is not None and eonr > 0:
            deviation_pct = abs(current_n - eonr) / eonr * 100
            score = float(np.clip(deviation_pct, 0, 100))
        else:
            score = 50.0

        return {
            "score": round(score, 2),
            "country": country,
            "n_obs": n,
            "current_avg_n_kg_ha": round(current_n, 1),
            "prices": {
                "fertilizer_per_kg": round(pf, 3),
                "crop_per_kg": round(pc, 3),
                "price_ratio": round(price_ratio, 3),
            },
            "best_model": best_name,
            "eonr_kg_ha": round(float(eonr), 1) if eonr is not None else None,
            "mvp_at_current_rate": round(float(mvp_current), 3) if mvp_current is not None else None,
            "vcr": round(float(vcr), 2) if vcr is not None else None,
            "models": {
                name: {k: (round(v, 4) if isinstance(v, float) else v)
                       for k, v in m.items()}
                for name, m in models.items()
            },
        }

    @staticmethod
    def _fit_quadratic(
        N: np.ndarray, Y: np.ndarray, price_ratio: float
    ) -> dict | None:
        """Fit quadratic response: y = a + b*N + c*N^2."""
        try:
            X = np.column_stack([np.ones(len(N)), N, N ** 2])
            beta = np.linalg.lstsq(X, Y, rcond=None)[0]
            a, b, c = beta
            fitted = X @ beta
            ss_res = np.sum((Y - fitted) ** 2)
            ss_tot = np.sum((Y - Y.mean()) ** 2)
            r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

            # EONR: dy/dN = b + 2c*N = price_ratio => N* = (PR - b) / (2c)
            eonr = None
            if c < 0:
                eonr = (price_ratio - b) / (2 * c)
                eonr = max(0, eonr)

            # MVP at current mean N
            n_mean = float(N.mean())
            mvp = b + 2 * c * n_mean

            return {
                "a": float(a), "b": float(b), "c": float(c),
                "r_squared": float(r2),
                "eonr": float(eonr) if eonr is not None else None,
                "mvp_at_current": float(mvp),
                "y_at_optimum": float(a + b * eonr + c * eonr ** 2) if eonr is not None else None,
            }
        except (np.linalg.LinAlgError, ValueError):
            return None

    @staticmethod
    def _fit_mitscherlich(
        N: np.ndarray, Y: np.ndarray, price_ratio: float
    ) -> dict | None:
        """Fit Mitscherlich: y = A * (1 - exp(-c * (N + b)))."""
        try:
            def model(N, A, c, b):
                return A * (1 - np.exp(-c * (N + b)))

            p0 = [float(Y.max()) * 1.1, 0.01, 10.0]
            bounds = ([0, 1e-6, -100], [Y.max() * 5, 1.0, 500])
            popt, _ = optimize.curve_fit(model, N, Y, p0=p0, bounds=bounds, maxfev=5000)
            A, c, b = popt

            fitted = model(N, A, c, b)
            ss_res = np.sum((Y - fitted) ** 2)
            ss_tot = np.sum((Y - Y.mean()) ** 2)
            r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

            # dy/dN = A * c * exp(-c * (N + b)) = price_ratio
            # N* = -b - ln(PR / (A*c)) / c
            if A * c > price_ratio > 0:
                eonr = -b - np.log(price_ratio / (A * c)) / c
                eonr = max(0, eonr)
            else:
                eonr = None

            n_mean = float(N.mean())
            mvp = A * c * np.exp(-c * (n_mean + b))

            return {
                "A": float(A), "c": float(c), "b": float(b),
                "r_squared": float(r2),
                "eonr": float(eonr) if eonr is not None else None,
                "mvp_at_current": float(mvp),
                "y_at_optimum": float(model(eonr, A, c, b)) if eonr is not None else None,
            }
        except (RuntimeError, ValueError, np.linalg.LinAlgError):
            return None

    @staticmethod
    def _fit_linear_plateau(
        N: np.ndarray, Y: np.ndarray, price_ratio: float
    ) -> dict | None:
        """Fit linear-plateau: y = min(a + b*N, y_plateau)."""
        try:
            # Grid search over breakpoints
            best_sse = float("inf")
            best_params = None
            n_sorted = np.sort(np.unique(N))
            if len(n_sorted) < 3:
                return None

            for bp_idx in range(1, len(n_sorted) - 1):
                bp = n_sorted[bp_idx]
                below = N <= bp
                above = N > bp

                if below.sum() < 2 or above.sum() < 1:
                    continue

                # Fit linear part below breakpoint
                X_lin = np.column_stack([np.ones(below.sum()), N[below]])
                try:
                    beta_lin = np.linalg.lstsq(X_lin, Y[below], rcond=None)[0]
                except np.linalg.LinAlgError:
                    continue
                a, b_coef = beta_lin
                plateau = a + b_coef * bp

                fitted = np.where(N <= bp, a + b_coef * N, plateau)
                sse = float(np.sum((Y - fitted) ** 2))
                if sse < best_sse:
                    best_sse = sse
                    best_params = (a, b_coef, plateau, bp)

            if best_params is None:
                return None

            a, b_coef, plateau, breakpoint = best_params
            fitted = np.where(N <= breakpoint, a + b_coef * N, plateau)
            ss_res = np.sum((Y - fitted) ** 2)
            ss_tot = np.sum((Y - Y.mean()) ** 2)
            r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

            # EONR for linear-plateau: if slope > price_ratio, apply up to breakpoint
            eonr = breakpoint if b_coef > price_ratio else 0.0

            n_mean = float(N.mean())
            mvp = b_coef if n_mean <= breakpoint else 0.0

            return {
                "intercept": float(a), "slope": float(b_coef),
                "plateau_yield": float(plateau),
                "breakpoint_n_kg_ha": float(breakpoint),
                "r_squared": float(r2),
                "eonr": float(eonr),
                "mvp_at_current": float(mvp),
            }
        except Exception:
            return None
