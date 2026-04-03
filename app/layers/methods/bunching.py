"""Bunching estimation at kink and notch points (Saez 2010, Kleven & Waseem 2013).

Bunching methods exploit clustering of agents at kink or notch points in
piecewise-linear budget sets (e.g. tax brackets, regulatory thresholds) to
recover structural behavioral elasticities without exogenous variation.

At a kink point (where marginal tax rate jumps):
    - Excess mass B = integral of bunching above counterfactual density
    - Elasticity e = B / (z* * log(1-t1)/(1-t0)) in the Saez (2010) formula

At a notch point (where average tax rate jumps):
    - Both bunching below and a hole above the threshold
    - Structural parameter recovery accounts for the dominated region

Counterfactual density is estimated by fitting a polynomial to the observed
density excluding the bunching region, then integrating the excess mass.

Key implementation:
    1. Histogram the running variable around the threshold
    2. Fit polynomial of degree q to bins outside the excluded region
    3. Compute excess mass B as observed - counterfactual in bunching region
    4. Bootstrap standard errors
    5. Recover structural elasticity from B

References:
    Saez, E. (2010). Do Taxpayers Bunch at Kink Points? AEJ: Economic
        Policy 2(3): 180-212.
    Chetty, R., Friedman, J., Olsen, T. & Pistaferri, L. (2011). Adjustment
        Costs, Firm Responses, and Micro vs. Macro Labor Supply Elasticities.
        QJE 126(2): 749-804.
    Kleven, H. & Waseem, M. (2013). Using Notches to Uncover Optimization
        Frictions and Structural Elasticities. QJE 128(2): 669-723.

Score: large excess mass relative to counterfactual -> high score (strong
behavioral response). No bunching -> STABLE (agents unresponsive to kink).
"""

import json

import numpy as np

from app.layers.base import LayerBase


class BunchingEstimation(LayerBase):
    layer_id = "l18"
    name = "Bunching Estimation"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        threshold = kwargs.get("threshold")
        bunching_type = kwargs.get("bunching_type", "kink")  # "kink" or "notch"
        poly_degree = kwargs.get("poly_degree", 7)
        exclude_width = kwargs.get("exclude_width", 2)
        n_bootstrap = kwargs.get("n_bootstrap", 200)

        rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'bunching'
            ORDER BY dp.value
            """,
            (country,),
        )

        if not rows or len(rows) < 50:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient microdata"}

        values = np.array([float(r["value"]) for r in rows if r["value"] is not None])
        if len(values) < 50:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient valid obs"}

        # Detect threshold from metadata if not provided
        if threshold is None:
            for r in rows:
                meta = json.loads(r["metadata"]) if r.get("metadata") else {}
                if meta.get("threshold"):
                    threshold = float(meta["threshold"])
                    break
        if threshold is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "threshold required"}

        # Also extract tax rates from metadata for elasticity
        t0, t1 = None, None
        for r in rows:
            meta = json.loads(r["metadata"]) if r.get("metadata") else {}
            if meta.get("rate_below") is not None:
                t0 = float(meta["rate_below"])
                t1 = float(meta["rate_above"])
                break

        # Bin the data
        n_bins = min(100, max(30, len(values) // 20))
        lo = float(np.percentile(values, 1))
        hi = float(np.percentile(values, 99))
        bin_edges = np.linspace(lo, hi, n_bins + 1)
        bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2.0
        counts, _ = np.histogram(values, bins=bin_edges)
        counts = counts.astype(float)

        # Normalize bin centers relative to threshold
        z = bin_centers - threshold
        bin_width = bin_edges[1] - bin_edges[0]

        # Exclude region around threshold
        exclude_lo = -exclude_width * bin_width
        exclude_hi = exclude_width * bin_width
        excluded = (z >= exclude_lo) & (z <= exclude_hi)
        included = ~excluded

        if np.sum(included) < poly_degree + 1:
            return {"score": None, "signal": "UNAVAILABLE", "error": "too few bins outside window"}

        # Fit counterfactual polynomial to non-excluded bins
        cf_counts = self._fit_counterfactual(z, counts, included, poly_degree)

        # Excess mass in excluded region
        bunching_region = excluded
        excess_mass = float(np.sum(counts[bunching_region] - cf_counts[bunching_region]))
        cf_mass_in_region = float(np.sum(cf_counts[bunching_region]))

        # Normalized excess mass b = B / counterfactual_height
        b_hat = excess_mass / cf_mass_in_region if cf_mass_in_region > 0 else 0.0

        # Bootstrap standard errors
        b_boots = []
        n_total = len(values)
        rng = np.random.default_rng(42)
        for _ in range(n_bootstrap):
            boot_vals = rng.choice(values, size=n_total, replace=True)
            boot_counts, _ = np.histogram(boot_vals, bins=bin_edges)
            boot_counts = boot_counts.astype(float)
            boot_cf = self._fit_counterfactual(z, boot_counts, included, poly_degree)
            boot_excess = float(np.sum(boot_counts[bunching_region] - boot_cf[bunching_region]))
            boot_cf_mass = float(np.sum(boot_cf[bunching_region]))
            if boot_cf_mass > 0:
                b_boots.append(boot_excess / boot_cf_mass)
        b_se = float(np.std(b_boots)) if b_boots else None

        # Structural elasticity (Saez formula for kink)
        elasticity = None
        if bunching_type == "kink" and t0 is not None and t1 is not None:
            log_ntr_ratio = np.log((1 - t1) / (1 - t0)) if (1 - t0) > 0 and (1 - t1) > 0 else None
            if log_ntr_ratio is not None and abs(log_ntr_ratio) > 1e-10:
                elasticity = b_hat / abs(log_ntr_ratio)

        # Score: large normalized excess mass = strong behavioral response
        abs_b = abs(b_hat)
        if abs_b > 3.0:
            score = 80.0
        elif abs_b > 1.5:
            score = 50.0 + (abs_b - 1.5) * 20.0
        elif abs_b > 0.5:
            score = 20.0 + (abs_b - 0.5) * 30.0
        else:
            score = abs_b * 40.0
        score = max(0.0, min(100.0, score))

        return {
            "score": round(score, 2),
            "country": country,
            "threshold": threshold,
            "bunching_type": bunching_type,
            "n_obs": len(values),
            "n_bins": n_bins,
            "poly_degree": poly_degree,
            "excess_mass": round(excess_mass, 2),
            "normalized_excess_mass": round(b_hat, 4),
            "se_normalized": round(b_se, 4) if b_se is not None else None,
            "elasticity": round(elasticity, 4) if elasticity is not None else None,
            "counterfactual_fit": {
                "r_squared": round(self._poly_r2(z, counts, included, poly_degree), 4),
            },
            "tax_rates": {"below": t0, "above": t1},
        }

    @staticmethod
    def _fit_counterfactual(z: np.ndarray, counts: np.ndarray,
                            included: np.ndarray, degree: int) -> np.ndarray:
        """Fit polynomial counterfactual density to non-excluded bins."""
        z_inc = z[included]
        c_inc = counts[included]
        coeffs = np.polyfit(z_inc, c_inc, degree)
        cf = np.polyval(coeffs, z)
        # Floor at zero (density cannot be negative)
        cf = np.maximum(cf, 0.0)
        return cf

    @staticmethod
    def _poly_r2(z: np.ndarray, counts: np.ndarray,
                 included: np.ndarray, degree: int) -> float:
        """R-squared of polynomial fit on included bins."""
        z_inc = z[included]
        c_inc = counts[included]
        coeffs = np.polyfit(z_inc, c_inc, degree)
        fitted = np.polyval(coeffs, z_inc)
        ss_res = float(np.sum((c_inc - fitted) ** 2))
        ss_tot = float(np.sum((c_inc - np.mean(c_inc)) ** 2))
        return 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
