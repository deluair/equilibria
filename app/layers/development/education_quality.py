"""Education quality: tertiary enrollment and primary completion rates.

Composite education outcomes module. Low primary completion signals basic
education failure; low tertiary enrollment signals human capital ceiling.
Both dimensions combined into a single development stress score.

Key references:
    Barro, R. & Lee, J.W. (2013). A new data set of educational attainment
        in the world, 1950-2010. Journal of Development Economics, 104, 184-198.
    UNESCO Institute for Statistics (2023). Global Education Monitoring Report.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

PRIMARY_TARGET = 95.0   # % completion benchmark
TERTIARY_TARGET = 40.0  # % enrollment benchmark (global average aspirational)


class EducationQuality(LayerBase):
    layer_id = "l4"
    name = "Education Quality"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Education quality composite from tertiary enrollment + primary completion.

        Queries SE.TER.ENRR (tertiary enrollment ratio %) and
        SE.PRM.CMPT.ZS (primary completion rate %).
        Score = 0.5 * primary_gap_score + 0.5 * tertiary_gap_score, clipped to 100.

        Returns dict with score, individual indicators, composite, and gaps.
        """
        country_iso3 = kwargs.get("country_iso3")

        tertiary_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SE.TER.ENRR'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        primary_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SE.PRM.CMPT.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        if not tertiary_rows and not primary_rows:
            return {"score": 50, "results": {"error": "no education data available"}}

        tertiary_data: dict[str, dict[str, float]] = {}
        for r in tertiary_rows:
            tertiary_data.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]

        primary_data: dict[str, dict[str, float]] = {}
        for r in primary_rows:
            primary_data.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]

        # Global medians for context
        def latest_val(data: dict[str, dict[str, float]]) -> list[float]:
            vals = []
            for iso_data in data.values():
                if iso_data:
                    yr = max(iso_data.keys())
                    if iso_data[yr] is not None:
                        vals.append(iso_data[yr])
            return vals

        tertiary_latest = latest_val(tertiary_data)
        primary_latest = latest_val(primary_data)
        global_tertiary_median = float(np.median(tertiary_latest)) if tertiary_latest else None
        global_primary_median = float(np.median(primary_latest)) if primary_latest else None

        # Target country
        target_analysis = None
        score = 50.0

        def get_latest(data: dict[str, dict[str, float]], iso: str) -> float | None:
            if iso not in data or not data[iso]:
                return None
            yr = max(data[iso].keys())
            return data[iso][yr]

        if country_iso3:
            tertiary_val = get_latest(tertiary_data, country_iso3)
            primary_val = get_latest(primary_data, country_iso3)

            primary_gap_score = None
            tertiary_gap_score = None

            if primary_val is not None:
                primary_gap_score = float(np.clip(max(0.0, PRIMARY_TARGET - primary_val) * 1.05, 0, 100))
            if tertiary_val is not None:
                tertiary_gap_score = float(np.clip(max(0.0, TERTIARY_TARGET - tertiary_val) * 2.5, 0, 100))

            if primary_gap_score is not None and tertiary_gap_score is not None:
                score = 0.5 * primary_gap_score + 0.5 * tertiary_gap_score
            elif primary_gap_score is not None:
                score = primary_gap_score
            elif tertiary_gap_score is not None:
                score = tertiary_gap_score

            score = float(np.clip(score, 0, 100))

            target_analysis = {
                "tertiary_enrollment_pct": tertiary_val,
                "primary_completion_pct": primary_val,
                "tertiary_target": TERTIARY_TARGET,
                "primary_target": PRIMARY_TARGET,
                "primary_gap_score": primary_gap_score,
                "tertiary_gap_score": tertiary_gap_score,
                "low_primary": primary_val is not None and primary_val < PRIMARY_TARGET,
                "low_tertiary": tertiary_val is not None and tertiary_val < TERTIARY_TARGET,
            }
        else:
            # Fallback: global average gaps
            if primary_latest:
                avg_primary_gap = float(np.mean([max(0.0, PRIMARY_TARGET - v) for v in primary_latest]))
                score = float(np.clip(avg_primary_gap * 1.05, 0, 100))

        return {
            "score": score,
            "results": {
                "primary_target_pct": PRIMARY_TARGET,
                "tertiary_target_pct": TERTIARY_TARGET,
                "global_tertiary_median": global_tertiary_median,
                "global_primary_median": global_primary_median,
                "n_tertiary_countries": len(tertiary_data),
                "n_primary_countries": len(primary_data),
                "target": target_analysis,
                "country_iso3": country_iso3,
            },
        }
