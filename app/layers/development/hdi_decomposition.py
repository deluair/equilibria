"""Human Development Index decomposition and contribution analysis.

Decomposes the UNDP Human Development Index into its three dimensions:
health (life expectancy), education (mean + expected years of schooling),
and standard of living (GNI per capita). Identifies binding constraints
to human development.

Key references:
    UNDP (2010). Human Development Report: The Real Wealth of Nations.
    Klugman, J., Rodriguez, F. & Choi, H. (2011). The HDI 2010: New
        controversies, old critiques. Journal of Economic Inequality, 9(2).
    Ravallion, M. (2012). Troubling tradeoffs in the Human Development Index.
        Journal of Development Economics, 99(2), 201-209.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

# UNDP goalposts for HDI normalization (2010 methodology)
HDI_GOALPOSTS = {
    "life_expectancy": {"min": 20.0, "max": 85.0},
    "expected_schooling": {"min": 0.0, "max": 18.0},
    "mean_schooling": {"min": 0.0, "max": 15.0},
    "gni_per_capita": {"min": 100.0, "max": 75000.0},
}


def _dimension_index(value: float, minimum: float, maximum: float, log: bool = False) -> float:
    """Compute dimension index using UNDP formula."""
    if log:
        if value <= 0:
            return 0.0
        value = np.log(value)
        minimum = np.log(minimum)
        maximum = np.log(maximum)
    denom = maximum - minimum
    if denom == 0:
        return 0.0
    return float(np.clip((value - minimum) / denom, 0, 1))


class HDIDecomposition(LayerBase):
    layer_id = "l4"
    name = "HDI Decomposition"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Decompose HDI into health, education, and income dimensions.

        Fetches life expectancy, schooling, and GNI per capita data.
        Computes dimension indices using UNDP goalposts and identifies
        the weakest dimension constraining overall development.

        Returns dict with score, dimension indices, HDI estimate,
        contribution analysis, and binding constraint identification.
        """
        country_iso3 = kwargs.get("country_iso3")

        # Fetch component indicators
        series_map = {
            "SP.DYN.LE00.IN": "life_expectancy",
            "SE.SCH.LIFE": "expected_schooling",
            "SE.ADT.LITR.ZS": "mean_schooling_proxy",  # Literacy as proxy
            "NY.GNP.PCAP.PP.KD": "gni_per_capita",
        }

        indicators: dict[str, dict[str, float]] = {}  # iso -> indicator -> value
        for series_id, label in series_map.items():
            rows = await db.fetch_all(
                """
                SELECT ds.country_iso3, dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.series_id = ?
                  AND dp.value IS NOT NULL
                  AND dp.date = (
                      SELECT MAX(dp2.date) FROM data_points dp2
                      WHERE dp2.series_id = ds.id
                  )
                """,
                (series_id,),
            )
            for r in rows:
                indicators.setdefault(r["country_iso3"], {})[label] = r["value"]

        if not indicators:
            return {"score": 50, "results": {"error": "no HDI component data"}}

        # Compute HDI for each country with sufficient data
        hdi_results: dict[str, dict] = {}
        for iso, data in indicators.items():
            le = data.get("life_expectancy")
            es = data.get("expected_schooling")
            ms_proxy = data.get("mean_schooling_proxy")
            gni = data.get("gni_per_capita")

            if le is None or gni is None:
                continue

            # Health index
            health_idx = _dimension_index(
                le, HDI_GOALPOSTS["life_expectancy"]["min"],
                HDI_GOALPOSTS["life_expectancy"]["max"],
            )

            # Education index: geometric mean of expected and mean schooling
            if es is not None:
                es_idx = _dimension_index(
                    es, HDI_GOALPOSTS["expected_schooling"]["min"],
                    HDI_GOALPOSTS["expected_schooling"]["max"],
                )
            else:
                es_idx = None

            if ms_proxy is not None:
                # Convert literacy rate to approximate mean years of schooling
                approx_ms = ms_proxy / 100 * 12  # rough approximation
                ms_idx = _dimension_index(
                    approx_ms, HDI_GOALPOSTS["mean_schooling"]["min"],
                    HDI_GOALPOSTS["mean_schooling"]["max"],
                )
            else:
                ms_idx = None

            if es_idx is not None and ms_idx is not None:
                education_idx = float(np.sqrt(es_idx * ms_idx))
            elif es_idx is not None:
                education_idx = es_idx
            elif ms_idx is not None:
                education_idx = ms_idx
            else:
                education_idx = None

            # Income index (log scale)
            income_idx = _dimension_index(
                gni, HDI_GOALPOSTS["gni_per_capita"]["min"],
                HDI_GOALPOSTS["gni_per_capita"]["max"],
                log=True,
            )

            # HDI: geometric mean of three dimension indices
            if education_idx is not None:
                hdi = float(np.power(health_idx * education_idx * income_idx, 1 / 3))
                dimensions = {
                    "health": health_idx,
                    "education": education_idx,
                    "income": income_idx,
                }
            else:
                # Two-dimension HDI (less accurate)
                hdi = float(np.sqrt(health_idx * income_idx))
                dimensions = {
                    "health": health_idx,
                    "income": income_idx,
                }

            # Identify binding constraint (lowest dimension)
            binding = min(dimensions, key=dimensions.get)

            # Contribution analysis: marginal gain from improving each dimension
            contributions = {}
            for dim, val in dimensions.items():
                if val < 0.999:
                    # 1% improvement in this dimension
                    improved = dict(dimensions)
                    improved[dim] = min(1.0, val * 1.01)
                    vals = list(improved.values())
                    new_hdi = float(np.power(np.prod(vals), 1 / len(vals)))
                    contributions[dim] = new_hdi - hdi
                else:
                    contributions[dim] = 0.0

            hdi_results[iso] = {
                "hdi": hdi,
                "dimensions": dimensions,
                "binding_constraint": binding,
                "contributions": contributions,
                "raw": {k: v for k, v in data.items()},
            }

        if not hdi_results:
            return {"score": 50, "results": {"error": "insufficient data for HDI computation"}}

        # Rankings
        ranked = sorted(hdi_results.items(), key=lambda x: x[1]["hdi"], reverse=True)
        rankings = {iso: rank + 1 for rank, (iso, _) in enumerate(ranked)}

        # Target country analysis
        target = hdi_results.get(country_iso3) if country_iso3 else None
        target_rank = rankings.get(country_iso3) if country_iso3 else None

        # Cross-country statistics
        all_hdi = [r["hdi"] for r in hdi_results.values()]
        binding_counts = {}
        for r in hdi_results.values():
            bc = r["binding_constraint"]
            binding_counts[bc] = binding_counts.get(bc, 0) + 1

        # Score based on target country HDI or global average
        if target:
            hdi_val = target["hdi"]
            if hdi_val >= 0.8:
                score = 20  # Very high HD
            elif hdi_val >= 0.7:
                score = 35  # High HD
            elif hdi_val >= 0.55:
                score = 55  # Medium HD
            else:
                score = 80  # Low HD
        else:
            avg_hdi = np.mean(all_hdi)
            score = 80 - avg_hdi * 70

        score = float(np.clip(score, 0, 100))

        results = {
            "target": target,
            "target_rank": target_rank,
            "global": {
                "mean_hdi": float(np.mean(all_hdi)),
                "median_hdi": float(np.median(all_hdi)),
                "std_hdi": float(np.std(all_hdi)),
                "min_hdi": float(np.min(all_hdi)),
                "max_hdi": float(np.max(all_hdi)),
                "binding_constraints": binding_counts,
            },
            "top_5": [(iso, r["hdi"]) for iso, r in ranked[:5]],
            "bottom_5": [(iso, r["hdi"]) for iso, r in ranked[-5:]],
            "n_countries": len(hdi_results),
            "country_iso3": country_iso3,
        }

        return {"score": score, "results": results}
