"""Multidimensional Poverty Index (MPI) computation using Alkire-Foster method.

Computes the MPI across 10 indicators in 3 dimensions (health, education,
living standards). Reports the incidence of poverty (headcount H),
intensity (average deprivation share A), and the MPI product (H x A).

Key references:
    Alkire, S. & Foster, J. (2011). Counting and multidimensional poverty
        measurement. Journal of Public Economics, 95(7-8), 476-487.
    Alkire, S. & Santos, M. (2014). Measuring acute poverty in the developing
        world. World Development, 59, 251-274.
    OPHI & UNDP (2023). Global Multidimensional Poverty Index.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

# MPI indicator structure: (series_id, dimension, weight, deprivation_threshold, direction)
# direction: "below" means deprived if value < threshold, "above" means deprived if value > threshold
MPI_INDICATORS = [
    # Health (1/3 total, 1/6 each)
    ("SH.DYN.MORT", "health", 1 / 6, 70, "above"),       # Child mortality > 70 per 1000
    ("SN.ITK.DEFC.ZS", "health", 1 / 6, 20, "above"),    # Undernourishment > 20%

    # Education (1/3 total, 1/6 each)
    ("SE.ADT.LITR.ZS", "education", 1 / 6, 50, "below"),  # Literacy < 50%
    ("SE.PRM.CMPT.ZS", "education", 1 / 6, 60, "below"),  # Primary completion < 60%

    # Living Standards (1/3 total, 1/18 each for 6 indicators)
    ("EG.ELC.ACCS.ZS", "living_standards", 1 / 18, 50, "below"),  # Electricity < 50%
    ("SH.H2O.SMDW.ZS", "living_standards", 1 / 18, 50, "below"),  # Safe water < 50%
    ("SH.STA.SMSS.ZS", "living_standards", 1 / 18, 50, "below"),  # Sanitation < 50%
    ("EG.CFT.ACCS.ZS", "living_standards", 1 / 18, 50, "below"),  # Clean fuel < 50%
    ("IT.CEL.SETS.P2", "living_standards", 1 / 18, 20, "below"),  # Mobile subs < 20 per 100
    ("EN.POP.SLUM.UR.ZS", "living_standards", 1 / 18, 40, "above"),  # Slum pop > 40%
]

# Poverty cutoff: deprived in at least 1/3 of weighted indicators
POVERTY_CUTOFF = 1 / 3


class MultidimensionalPoverty(LayerBase):
    layer_id = "l4"
    name = "Multidimensional Poverty (MPI)"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Compute MPI using Alkire-Foster dual-cutoff method.

        For each country, assesses deprivation across 10 indicators in 3
        dimensions. A country is MPI-poor if its weighted deprivation
        count exceeds the poverty cutoff (1/3).

        Returns dict with score, MPI (H x A), headcount ratio, intensity,
        dimensional breakdown, and deprivation profile.
        """
        country_iso3 = kwargs.get("country_iso3")

        # Fetch all indicator data
        indicator_data: dict[str, dict[str, float]] = {}  # iso -> series_id -> value
        for series_id, dim, weight, threshold, direction in MPI_INDICATORS:
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
                indicator_data.setdefault(r["country_iso3"], {})[series_id] = r["value"]

        if not indicator_data:
            return {"score": 50, "results": {"error": "no MPI indicator data"}}

        # Compute MPI for each country
        country_mpi: dict[str, dict] = {}
        for iso, data in indicator_data.items():
            deprivation_score = 0.0
            deprivation_count = 0
            total_weight = 0.0
            deprivations = {}
            dimensional_scores = {"health": 0.0, "education": 0.0, "living_standards": 0.0}
            dimensional_weights = {"health": 0.0, "education": 0.0, "living_standards": 0.0}

            for series_id, dim, weight, threshold, direction in MPI_INDICATORS:
                if series_id not in data:
                    continue

                value = data[series_id]
                total_weight += weight

                if direction == "below":
                    deprived = value < threshold
                else:
                    deprived = value > threshold

                if deprived:
                    deprivation_score += weight
                    deprivation_count += 1
                    dimensional_scores[dim] += weight

                dimensional_weights[dim] += weight
                deprivations[series_id] = {
                    "value": value,
                    "threshold": threshold,
                    "deprived": deprived,
                    "dimension": dim,
                    "weight": weight,
                }

            if total_weight == 0:
                continue

            # Normalize deprivation score
            ci = deprivation_score / total_weight if total_weight > 0 else 0

            # Is MPI-poor? (deprivation score >= cutoff)
            is_poor = ci >= POVERTY_CUTOFF

            # Headcount (H): % of countries that are MPI-poor (at country level,
            # this is binary; cross-country aggregation provides the headcount)
            # Intensity (A): average deprivation share among the poor
            country_mpi[iso] = {
                "deprivation_score": ci,
                "is_mpi_poor": is_poor,
                "deprivation_count": deprivation_count,
                "total_indicators_available": sum(1 for s, _, _, _, _ in MPI_INDICATORS if s in data),
                "deprivations": deprivations,
                "dimensional_contribution": {
                    dim: dimensional_scores[dim] / deprivation_score if deprivation_score > 0 else 0
                    for dim in dimensional_scores
                },
                "dimensional_deprivation": {
                    dim: dimensional_scores[dim] / dimensional_weights[dim]
                    if dimensional_weights[dim] > 0 else 0
                    for dim in dimensional_scores
                },
            }

        if not country_mpi:
            return {"score": 50, "results": {"error": "insufficient data for MPI computation"}}

        # Cross-country aggregation
        poor_countries = [iso for iso, r in country_mpi.items() if r["is_mpi_poor"]]
        headcount_ratio = len(poor_countries) / len(country_mpi)

        if poor_countries:
            intensity = np.mean([country_mpi[iso]["deprivation_score"] for iso in poor_countries])
        else:
            intensity = 0.0

        mpi = headcount_ratio * intensity

        # Rankings (higher deprivation = worse)
        ranked = sorted(country_mpi.items(), key=lambda x: x[1]["deprivation_score"])
        rankings = {iso: rank + 1 for rank, (iso, _) in enumerate(ranked)}

        # Target country
        target = country_mpi.get(country_iso3) if country_iso3 else None
        target_rank = rankings.get(country_iso3) if country_iso3 else None

        # Score: high MPI = high score (stress), low MPI = low (stable)
        if target:
            ci = target["deprivation_score"]
            if ci < 0.1:
                score = 15
            elif ci < POVERTY_CUTOFF:
                score = 35
            elif ci < 0.5:
                score = 60
            else:
                score = 85
        else:
            score = mpi * 200  # Scale 0-0.5 to 0-100
            score = float(np.clip(score, 10, 90))

        results = {
            "global_mpi": float(mpi),
            "headcount_ratio": float(headcount_ratio),
            "intensity": float(intensity),
            "n_poor_countries": len(poor_countries),
            "n_countries": len(country_mpi),
            "poverty_cutoff": POVERTY_CUTOFF,
            "target": target,
            "target_rank": target_rank,
            "most_deprived": [(iso, r["deprivation_score"]) for iso, r in ranked[-5:]],
            "least_deprived": [(iso, r["deprivation_score"]) for iso, r in ranked[:5]],
            "country_iso3": country_iso3,
        }

        return {"score": float(np.clip(score, 0, 100)), "results": results}
