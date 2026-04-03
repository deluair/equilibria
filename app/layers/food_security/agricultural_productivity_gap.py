"""Agricultural yield gap from global benchmark (cereal yield).

The yield gap measures how far a country's cereal yields are below the
global average benchmark, representing unrealized production potential and
signaling structural agricultural underperformance.

Methodology:
    Fetch WDI indicator AG.YLD.CREL.KG (cereal yield, kg per hectare).

    Benchmark = 5000 kg/ha (approximate global average, World Bank 2020).

    score = clip(max(0, 5000 - yield_kg_ha) / 50, 0, 100)

    Interpretation:
        yield = 5000 kg/ha: score = 0 (at or above benchmark)
        yield = 2500 kg/ha: score = 50 (moderate gap)
        yield = 0 kg/ha:   score = 100 (maximum gap)

Score (0-100): Higher score = greater yield gap (more productivity stress).

References:
    World Bank (2023). WDI: AG.YLD.CREL.KG.
    van Ittersum, M.K. et al. (2013). "Yield gap analysis with local to
        global relevance." Field Crops Research, 143, 4-17.
    FAO (2017). "The Future of Food and Agriculture: Trends and Challenges."
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class AgriculturalProductivityGap(LayerBase):
    layer_id = "lFS"
    name = "Agricultural Productivity Gap"

    BENCHMARK_KG_HA = 5000.0

    async def compute(self, db, **kwargs) -> dict:
        """Compute cereal yield gap and stress score.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
            benchmark_kg_ha : float - yield benchmark (default 5000)
        """
        country = kwargs.get("country_iso3", "BGD")
        benchmark = float(kwargs.get("benchmark_kg_ha", self.BENCHMARK_KG_HA))

        row = await db.fetch_one(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.indicator_code = 'AG.YLD.CREL.KG'
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )
        if not row:
            row = await db.fetch_one(
                """
                SELECT dp.value, dp.date
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.name LIKE '%cereal%yield%'
                ORDER BY dp.date DESC
                LIMIT 1
                """,
                (country,),
            )

        if not row or row["value"] is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no cereal yield data available",
            }

        yield_kg_ha = float(row["value"])
        gap = max(0.0, benchmark - yield_kg_ha)
        score = float(np.clip(gap / 50.0, 0.0, 100.0))

        performance_ratio = yield_kg_ha / benchmark if benchmark > 0 else None
        above_benchmark = yield_kg_ha >= benchmark

        return {
            "score": round(score, 2),
            "country": country,
            "yield_kg_ha": round(yield_kg_ha, 1),
            "benchmark_kg_ha": benchmark,
            "yield_gap_kg_ha": round(gap, 1),
            "performance_ratio": round(performance_ratio, 4) if performance_ratio is not None else None,
            "above_benchmark": above_benchmark,
            "data_date": row["date"],
            "indicator": "AG.YLD.CREL.KG",
        }
