"""Under-5 child mortality rate vs SDG benchmark.

Under-5 mortality rate (U5MR) measures deaths per 1,000 live births before
age five. The SDG 3.2 target is to end preventable deaths: U5MR < 25 per
1,000 live births by 2030. High U5MR reflects malnutrition, infectious
disease burden, weak primary care, and poor sanitation.

High score = high stress.

References:
    IGME (2023). Levels & Trends in Child Mortality. UNICEF/WHO/World Bank/UN DESA.
    UN SDG 3.2: End preventable under-5 deaths, target U5MR < 25 per 1,000.

Sources: WDI 'SH.DYN.MORT' (under-5 mortality rate per 1,000 live births).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class ChildMortality(LayerBase):
    layer_id = "l8"
    name = "Child Mortality"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Score under-5 mortality against the SDG 3.2 benchmark.

        Fetches U5MR (SH.DYN.MORT). Score rises from 0 (U5MR = 0) to 100
        (U5MR = 250, i.e. 10x SDG target).
        Formula: score = clip(u5mr / 2.5, 0, 100).
        """
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SH.DYN.MORT'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no child mortality data (SH.DYN.MORT)",
            }

        latest = rows[0]
        u5mr = float(latest["value"])
        year = latest["date"][:4]

        # Score: 0 at U5MR=0, 100 at U5MR=250 (10x SDG target of 25)
        score = float(min(max(u5mr / 2.5, 0.0), 100.0))

        sdg_met = u5mr < 25.0

        return {
            "score": score,
            "results": {
                "country": country,
                "year": year,
                "u5mr_per_1000": u5mr,
                "sdg_target": 25.0,
                "sdg_met": sdg_met,
                "gap_to_sdg": max(0.0, u5mr - 25.0),
            },
        }
