"""Enrollment shortfall vs universal targets.

Measures the gap between actual gross or net enrollment ratios and the
universal target (100%). Decomposed by level (primary, secondary, tertiary).
Large enrollment gaps signal human capital formation failure and foregone
growth potential.

References:
    UNESCO Institute for Statistics (2023). SDG 4 global and thematic
        monitoring framework indicators.
    Barro, R. & Lee, J.W. (2013). A new data set of educational attainment
        in the world, 1950-2010. JDE, 104, 184-198.

Score: total weighted enrollment gap; large gap -> STRESS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class SchoolEnrollmentGap(LayerBase):
    layer_id = "lED"
    name = "School Enrollment Gap"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        # WDI series: gross enrollment ratios
        series_map = {
            "primary": "SE.ENR.PRIM.FM.ZS",
            "secondary": "SE.ENR.SECD.FM.ZS",
            "tertiary": "SE.ENR.TERT.FM.ZS",
        }

        enrollment = {}
        for level, sid in series_map.items():
            rows = await db.fetch_all(
                """
                SELECT dp.value, dp.date
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                ORDER BY dp.date DESC
                LIMIT 3
                """,
                (country, sid),
            )
            vals = [r["value"] for r in rows if r["value"] is not None]
            enrollment[level] = vals[0] if vals else None

        # Also try net enrollment for primary (SE.PRM.NENR)
        net_rows = await db.fetch_all(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SE.PRM.NENR'
            ORDER BY dp.date DESC
            LIMIT 3
            """,
            (country,),
        )
        net_vals = [r["value"] for r in net_rows if r["value"] is not None]
        net_primary = net_vals[0] if net_vals else None

        if all(v is None for v in enrollment.values()) and net_primary is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no enrollment data",
            }

        # Weights: primary 0.4, secondary 0.4, tertiary 0.2
        # Target = 100 for primary/secondary, 50 for tertiary
        weights = {"primary": 0.40, "secondary": 0.40, "tertiary": 0.20}
        targets = {"primary": 100.0, "secondary": 100.0, "tertiary": 50.0}

        gaps = {}
        weighted_gap = 0.0
        weight_sum = 0.0
        for level in ["primary", "secondary", "tertiary"]:
            val = enrollment[level]
            if val is not None:
                gap = max(0.0, targets[level] - val)
                gaps[level] = round(gap, 2)
                weighted_gap += weights[level] * gap
                weight_sum += weights[level]

        if weight_sum == 0:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no computable gaps"}

        avg_gap = weighted_gap / weight_sum
        # Score scales with gap: 0 gap = 0 stress, 50pp gap = 100 stress
        score = min(100.0, avg_gap * 2.0)

        return {
            "score": round(score, 2),
            "country": country,
            "enrollment_rates": {k: (round(v, 2) if v is not None else None) for k, v in enrollment.items()},
            "net_primary_enrollment": round(net_primary, 2) if net_primary is not None else None,
            "gaps_from_target": gaps,
            "weighted_avg_gap_pp": round(avg_gap, 2),
        }
