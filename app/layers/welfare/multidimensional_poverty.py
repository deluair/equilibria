"""Multidimensional Poverty module.

Alkire-Foster (2011) style MPI proxy using health, education, and living
standards deprivation. Each indicator below its threshold contributes to
the deprivation score.

Indicators:
  - SP.DYN.IMRT.IN  : infant mortality rate (per 1,000 live births) — health
  - SE.PRM.CMPT.ZS  : primary school completion rate (%) — education
  - EG.ELC.ACCS.ZS  : access to electricity (% of population) — living standards

Deprivation thresholds:
  - Infant mortality > 20 per 1,000 -> deprived
  - Primary completion < 90% -> deprived
  - Electricity access < 90% -> deprived

Score = weighted sum of deprivation intensities, clipped to [0, 100].

Sources: WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

# Deprivation thresholds
_IMRT_THRESHOLD = 20.0      # per 1,000 live births
_CMPT_THRESHOLD = 90.0      # primary completion rate %
_ELEC_THRESHOLD = 90.0      # electricity access %

# Dimension weights (equal per Alkire-Foster)
_WEIGHTS = {"health": 1 / 3, "education": 1 / 3, "living_standards": 1 / 3}


class MultidimensionalPoverty(LayerBase):
    layer_id = "lWE"
    name = "Multidimensional Poverty"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        series_map = {
            "infant_mortality": "SP.DYN.IMRT.IN",
            "primary_completion": "SE.PRM.CMPT.ZS",
            "electricity_access": "EG.ELC.ACCS.ZS",
        }

        latest: dict[str, float | None] = {}
        dates: dict[str, str | None] = {}

        for key, sid in series_map.items():
            rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                ORDER BY dp.date DESC
                LIMIT 1
                """,
                (country, sid),
            )
            if rows:
                latest[key] = float(rows[0]["value"])
                dates[key] = rows[0]["date"]
            else:
                latest[key] = None
                dates[key] = None

        available = {k: v for k, v in latest.items() if v is not None}
        if len(available) == 0:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no MPI dimension data available",
            }

        deprivation_intensities: dict[str, float] = {}

        # Health: infant mortality above threshold
        if latest["infant_mortality"] is not None:
            imrt = latest["infant_mortality"]
            if imrt > _IMRT_THRESHOLD:
                # Intensity: how far above threshold, normalized to [0,1] over range [20, 100]
                intensity = float(np.clip((imrt - _IMRT_THRESHOLD) / (100.0 - _IMRT_THRESHOLD), 0, 1))
            else:
                intensity = 0.0
            deprivation_intensities["health"] = intensity

        # Education: primary completion below threshold
        if latest["primary_completion"] is not None:
            cmpt = latest["primary_completion"]
            if cmpt < _CMPT_THRESHOLD:
                intensity = float(np.clip((_CMPT_THRESHOLD - cmpt) / _CMPT_THRESHOLD, 0, 1))
            else:
                intensity = 0.0
            deprivation_intensities["education"] = intensity

        # Living standards: electricity access below threshold
        if latest["electricity_access"] is not None:
            elec = latest["electricity_access"]
            if elec < _ELEC_THRESHOLD:
                intensity = float(np.clip((_ELEC_THRESHOLD - elec) / _ELEC_THRESHOLD, 0, 1))
            else:
                intensity = 0.0
            deprivation_intensities["living_standards"] = intensity

        if not deprivation_intensities:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient dimension data",
            }

        # Weighted deprivation index
        total_weight = sum(_WEIGHTS[k] for k in deprivation_intensities)
        mpi_proxy = sum(
            _WEIGHTS[k] * v for k, v in deprivation_intensities.items()
        ) / total_weight

        score = float(np.clip(mpi_proxy * 100, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "mpi_proxy": round(mpi_proxy, 4),
            "deprivation_intensities": {k: round(v, 4) for k, v in deprivation_intensities.items()},
            "infant_mortality": round(latest["infant_mortality"], 2) if latest["infant_mortality"] is not None else None,
            "infant_mortality_date": dates["infant_mortality"],
            "primary_completion_pct": round(latest["primary_completion"], 2) if latest["primary_completion"] is not None else None,
            "primary_completion_date": dates["primary_completion"],
            "electricity_access_pct": round(latest["electricity_access"], 2) if latest["electricity_access"] is not None else None,
            "electricity_access_date": dates["electricity_access"],
            "n_dimensions": len(deprivation_intensities),
            "method": "Alkire-Foster MPI proxy; equal-weighted deprivation intensities across 3 dimensions",
            "reference": "Alkire & Foster 2011; UNDP MPI methodology",
        }
