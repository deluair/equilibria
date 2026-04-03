"""Signaling Model module.

Evaluates education signal quality via Spence (1973) signaling theory.

If tertiary enrollment rises while productivity growth stagnates or falls,
education is functioning primarily as a signal (credential) rather than
building real human capital. High enrollment + low productivity divergence
= signaling without substance.

Score = enrollment_growth_advantage - productivity_growth,
normalized and clipped to [0, 100].

Sources: WDI (SE.TER.ENRR, NY.GDP.PCAP.KD.ZG)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class SignalingModel(LayerBase):
    layer_id = "lGT"
    name = "Signaling Model"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        enrollment_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SE.TER.ENRR'
            ORDER BY dp.date
            """,
            (country,),
        )

        prod_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.PCAP.KD.ZG'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not enrollment_rows or len(enrollment_rows) < 5:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data: need SE.TER.ENRR (min 5 obs)",
            }

        if not prod_rows or len(prod_rows) < 5:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data: need NY.GDP.PCAP.KD.ZG (min 5 obs)",
            }

        enr_map = {r["date"]: float(r["value"]) for r in enrollment_rows}
        prod_map = {r["date"]: float(r["value"]) for r in prod_rows}
        common = sorted(set(enr_map) & set(prod_map))

        if len(common) < 5:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "fewer than 5 overlapping dates",
            }

        enr_arr = np.array([enr_map[d] for d in common])
        prod_arr = np.array([prod_map[d] for d in common])
        t = np.arange(len(common), dtype=float)

        enr_slope = float(np.polyfit(t, enr_arr, 1)[0])
        enr_mean = float(np.mean(enr_arr))
        enr_growth = enr_slope / max(abs(enr_mean), 1e-10)

        prod_mean = float(np.mean(prod_arr))

        # Signaling gap: strong enrollment growth + weak/negative productivity
        # Normalize enrollment growth rate to % per year equivalent
        enr_pct_growth = enr_growth * 100.0

        # Signaling stress: enrollment expanding while productivity lags
        # prod_mean already in %, range roughly -10 to +15
        gap = enr_pct_growth - prod_mean

        # Score: gap > 5 pp starts registering; gap > 20 pp = 100
        score = float(np.clip((gap - 5.0) / 15.0 * 100.0, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "tertiary_enrollment_pct_growth_per_yr": round(enr_pct_growth, 4),
            "gdp_per_capita_growth_mean_pct": round(prod_mean, 4),
            "signaling_gap_pp": round(gap, 4),
            "enrollment_mean": round(enr_mean, 2),
            "n_obs": len(common),
            "period": f"{common[0]} to {common[-1]}",
            "interpretation": (
                "education primarily credential signaling, not human capital" if score > 60
                else "moderate signaling vs human capital tension" if score > 30
                else "education aligned with productivity outcomes"
            ),
        }
