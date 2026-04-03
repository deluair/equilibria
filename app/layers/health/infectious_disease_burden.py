"""Infectious disease burden: TB incidence as proxy.

Tuberculosis (TB) incidence per 100,000 population is a reliable proxy for
infectious disease burden in low- and middle-income countries. TB thrives
where health systems are weak, malnutrition is high, and HIV is prevalent.
The End TB Strategy target is < 10 per 100,000 by 2035.

High score = high infectious disease stress.

References:
    WHO (2023). Global Tuberculosis Report 2023. Geneva.
    WHO End TB Strategy targets: 90% reduction in TB deaths and 80% reduction
        in TB incidence by 2030 relative to 2015.

Sources: WDI 'SH.TBS.INCD' (TB incidence per 100,000 population).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class InfectiousDiseaseBurden(LayerBase):
    layer_id = "l8"
    name = "Infectious Disease Burden"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Score infectious disease burden via TB incidence rate.

        Formula: score = clip(tb_per_100k / 5, 0, 100).
        At TB = 500 per 100k the score reaches 100 (crisis level).
        Low-burden countries (< 10/100k) score below 2.
        """
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SH.TBS.INCD'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no TB incidence data (SH.TBS.INCD)",
            }

        latest = rows[0]
        tb_incidence = float(latest["value"])
        year = latest["date"][:4]

        # Score: 0 at 0/100k, 100 at 500/100k
        score = float(min(max(tb_incidence / 5.0, 0.0), 100.0))

        who_category = (
            "high burden" if tb_incidence >= 150
            else "upper-middle burden" if tb_incidence >= 50
            else "lower-middle burden" if tb_incidence >= 10
            else "low burden"
        )

        return {
            "score": score,
            "results": {
                "country": country,
                "year": year,
                "tb_incidence_per_100k": tb_incidence,
                "end_tb_target": 10.0,
                "who_category": who_category,
                "above_end_tb_target": tb_incidence > 10.0,
            },
        }
