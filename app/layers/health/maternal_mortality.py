"""Maternal mortality ratio vs SDG benchmark.

Maternal mortality ratio (MMR) measures deaths per 100,000 live births.
The SDG 3.1 target is to reduce MMR to below 70 per 100,000 by 2030.
High MMR reflects failures across the health system: access to skilled
birth attendance, emergency obstetric care, antenatal coverage, and
underlying malnutrition or anaemia.

High score = high stress (crisis-level MMR).

References:
    WHO, UNICEF, UNFPA, World Bank (2023). Trends in maternal mortality:
        2000 to 2020. WHO.
    UN SDG 3.1: Reduce global MMR to < 70 per 100,000 live births by 2030.

Sources: WDI 'SH.STA.MMRT' (maternal mortality ratio, per 100k live births).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class MaternalMortality(LayerBase):
    layer_id = "l8"
    name = "Maternal Mortality"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Score maternal mortality ratio against the SDG 3.1 benchmark.

        Fetches MMR (SH.STA.MMRT) for the target country. Score rises
        linearly from 0 (MMR = 0) to 100 (MMR = 700, i.e. 10x SDG target).
        Formula: score = clip(mmr / 7, 0, 100).
        """
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SH.STA.MMRT'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no maternal mortality data (SH.STA.MMRT)",
            }

        latest = rows[0]
        mmr = float(latest["value"])
        year = latest["date"][:4]

        # Score: 0 at MMR=0 (no deaths), 100 at MMR=700 (10x SDG limit)
        score = float(min(max(mmr / 7.0, 0.0), 100.0))

        sdg_met = mmr < 70.0

        return {
            "score": score,
            "results": {
                "country": country,
                "year": year,
                "mmr_per_100k": mmr,
                "sdg_target": 70.0,
                "sdg_met": sdg_met,
                "gap_to_sdg": max(0.0, mmr - 70.0),
            },
        }
