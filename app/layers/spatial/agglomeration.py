"""Duranton-Puga agglomeration economies analysis.

Estimates agglomeration externalities through city size distributions, Zipf's law
testing, and spatial wage premiums. Agglomeration economies arise when firms and
workers benefit from spatial proximity (sharing, matching, learning mechanisms).

Zipf's law: the rank-size distribution of cities follows a power law with
exponent close to -1. Deviations signal urban primacy (exponent < -1) or
excessive fragmentation (exponent > -1).

    ln(rank) = a - zeta * ln(population)

Zipf holds when zeta is approximately 1.0 (Gabaix 1999).

Spatial wage premium: wages in larger cities exceed those in smaller ones even
after controlling for worker characteristics, reflecting agglomeration
externalities (Combes et al. 2008).

    ln(w_r) = b0 + b1*ln(density_r) + b2*X_r + e_r

where the elasticity b1 is typically 0.02-0.05 (doubling density raises wages
2-5%).

References:
    Duranton, G. & Puga, D. (2004). Micro-foundations of urban agglomeration
        economies. In Handbook of Regional and Urban Economics, Vol. 4.
    Gabaix, X. (1999). Zipf's Law for Cities: An Explanation. QJE 114(3).
    Combes, P.-P., Duranton, G. & Gobillon, L. (2008). Spatial Wage
        Disparities: Sorting Matters! Journal of Urban Economics 63(2).

Score: Zipf deviation + weak/absent wage premium -> STRESS.
"""

import json

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class Agglomeration(LayerBase):
    layer_id = "l11"
    name = "Agglomeration Economies"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        # --- Fetch city population data ---
        city_rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'city_population'
            ORDER BY dp.value DESC
            """,
            (country,),
        )

        if not city_rows or len(city_rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient city data"}

        populations = []
        for row in city_rows:
            pop = row["value"]
            if pop is not None and pop > 0:
                populations.append(pop)

        if len(populations) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient valid cities"}

        populations = np.array(sorted(populations, reverse=True))
        n_cities = len(populations)

        # --- Zipf's law test ---
        ranks = np.arange(1, n_cities + 1, dtype=float)
        ln_rank = np.log(ranks)
        ln_pop = np.log(populations)

        slope, intercept, r_value, p_value, std_err = stats.linregress(ln_pop, ln_rank)
        zeta = -slope  # Zipf exponent (should be ~1.0)
        zipf_r2 = r_value ** 2

        # Deviation from Zipf: |zeta - 1|
        zipf_deviation = abs(zeta - 1.0)

        # Urban primacy ratio: largest city / second largest
        primacy_ratio = float(populations[0] / populations[1]) if n_cities >= 2 else None

        # Herfindahl index for city size concentration
        pop_shares = populations / populations.sum()
        hhi = float(np.sum(pop_shares ** 2))

        # --- Spatial wage premium ---
        wage_rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'spatial_wages'
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        wage_premium = None
        wage_elasticity = None
        wage_n = 0

        if wage_rows and len(wage_rows) >= 10:
            wages = []
            densities = []
            for row in wage_rows:
                w = row["value"]
                meta = json.loads(row["metadata"]) if row.get("metadata") else {}
                d = meta.get("density")
                if w is not None and w > 0 and d is not None and d > 0:
                    wages.append(np.log(w))
                    densities.append(np.log(d))

            if len(wages) >= 10:
                wages_arr = np.array(wages)
                densities_arr = np.array(densities)
                X = np.column_stack([np.ones(len(densities_arr)), densities_arr])
                beta = np.linalg.lstsq(X, wages_arr, rcond=None)[0]
                wage_elasticity = float(beta[1])
                # Premium: predicted wage at 90th vs 10th percentile density
                d_p90 = np.percentile(densities_arr, 90)
                d_p10 = np.percentile(densities_arr, 10)
                wage_premium = float(np.exp(beta[1] * (d_p90 - d_p10)) - 1.0)
                wage_n = len(wages)

        # --- Score ---
        # Zipf deviation contributes 60%, wage premium absence 40%
        zipf_score = min(100.0, zipf_deviation * 100.0)  # |zeta-1|=1 -> score 100

        if wage_elasticity is not None:
            # Healthy elasticity is 0.02-0.05; below 0.01 or negative is concerning
            if wage_elasticity < 0.01:
                wage_score = 70.0
            elif wage_elasticity < 0.02:
                wage_score = 50.0
            elif wage_elasticity <= 0.06:
                wage_score = 20.0
            else:
                wage_score = 40.0  # Implausibly high
        else:
            wage_score = 50.0  # No data, neutral

        score = 0.6 * zipf_score + 0.4 * wage_score
        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "n_cities": n_cities,
            "zipf": {
                "zeta": round(zeta, 4),
                "std_err": round(std_err, 4),
                "p_value": round(p_value, 6),
                "r_squared": round(zipf_r2, 4),
                "deviation_from_unity": round(zipf_deviation, 4),
            },
            "city_distribution": {
                "primacy_ratio": round(primacy_ratio, 2) if primacy_ratio else None,
                "hhi": round(hhi, 6),
                "largest_city_pop": round(float(populations[0]), 0),
                "median_city_pop": round(float(np.median(populations)), 0),
            },
            "wage_premium": {
                "density_elasticity": round(wage_elasticity, 4) if wage_elasticity else None,
                "p90_p10_premium": round(wage_premium, 4) if wage_premium else None,
                "n_obs": wage_n,
            },
        }
