"""Infrastructure quality: electricity access and paved roads composite.

Measures infrastructure development via electricity access and paved road
coverage. Low values on both dimensions signal infrastructure deficits that
constrain economic activity and productivity.

Key references:
    Calderón, C. & Servén, L. (2008). Infrastructure and economic development
        in Sub-Saharan Africa. Journal of African Economies, 17(S1), i13-i87.
    World Bank (2023). Infrastructure for Development.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

ELECTRICITY_TARGET = 95.0   # % population with access
ROADS_TARGET = 70.0          # % paved roads of total network


class InfrastructureQuality(LayerBase):
    layer_id = "l4"
    name = "Infrastructure Quality"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Infrastructure development proxy: electricity + paved roads composite.

        Queries EG.ELC.ACCS.ZS (access to electricity, % population) and
        IS.ROD.PAVE.ZS (paved roads, % of total roads).
        Electricity gap score = max(0, 95 - elec) * 1.05.
        Roads gap score = max(0, 70 - paved) * 1.43.
        Composite = 0.6 * elec_score + 0.4 * roads_score if both available.

        Returns dict with score, individual components, and infrastructure profile.
        """
        country_iso3 = kwargs.get("country_iso3")

        elec_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'EG.ELC.ACCS.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        roads_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'IS.ROD.PAVE.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        if not elec_rows and not roads_rows:
            return {"score": 50, "results": {"error": "no infrastructure data available"}}

        elec_data: dict[str, dict[str, float]] = {}
        for r in elec_rows:
            elec_data.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]

        roads_data: dict[str, dict[str, float]] = {}
        for r in roads_rows:
            roads_data.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]

        def latest_val(data: dict[str, dict[str, float]], iso: str) -> float | None:
            if iso not in data or not data[iso]:
                return None
            yr = max(data[iso].keys())
            return data[iso][yr]

        # Global medians
        elec_latest = [
            data[max(data.keys())]
            for data in elec_data.values()
            if data and data[max(data.keys())] is not None
        ]
        roads_latest = [
            data[max(data.keys())]
            for data in roads_data.values()
            if data and data[max(data.keys())] is not None
        ]
        global_elec_median = float(np.median(elec_latest)) if elec_latest else None
        global_roads_median = float(np.median(roads_latest)) if roads_latest else None

        # Target country
        target_analysis = None
        score = 50.0

        if country_iso3:
            elec_val = latest_val(elec_data, country_iso3)
            roads_val = latest_val(roads_data, country_iso3)

            elec_score = None
            roads_score = None

            if elec_val is not None:
                elec_score = float(np.clip(max(0.0, ELECTRICITY_TARGET - elec_val) * 1.05, 0, 100))
            if roads_val is not None:
                roads_score = float(np.clip(max(0.0, ROADS_TARGET - roads_val) * 1.43, 0, 100))

            if elec_score is not None and roads_score is not None:
                score = 0.6 * elec_score + 0.4 * roads_score
            elif elec_score is not None:
                score = elec_score
            elif roads_score is not None:
                score = roads_score

            score = float(np.clip(score, 0, 100))

            target_analysis = {
                "electricity_access_pct": elec_val,
                "paved_roads_pct": roads_val,
                "electricity_target_pct": ELECTRICITY_TARGET,
                "roads_target_pct": ROADS_TARGET,
                "electricity_gap_score": elec_score,
                "roads_gap_score": roads_score,
                "no_electricity_crisis": elec_val is not None and elec_val < 50,
                "poor_roads": roads_val is not None and roads_val < 30,
            }
        else:
            scores = []
            if elec_latest:
                avg_gap = float(np.mean([max(0.0, ELECTRICITY_TARGET - v) for v in elec_latest]))
                scores.append(avg_gap * 1.05)
            if roads_latest:
                avg_gap = float(np.mean([max(0.0, ROADS_TARGET - v) for v in roads_latest]))
                scores.append(avg_gap * 1.43)
            if scores:
                score = float(np.clip(np.mean(scores), 0, 100))

        return {
            "score": score,
            "results": {
                "electricity_target_pct": ELECTRICITY_TARGET,
                "roads_target_pct": ROADS_TARGET,
                "global_elec_median_pct": global_elec_median,
                "global_roads_median_pct": global_roads_median,
                "n_elec_countries": len(elec_data),
                "n_roads_countries": len(roads_data),
                "target": target_analysis,
                "country_iso3": country_iso3,
            },
        }
