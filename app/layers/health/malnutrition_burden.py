"""Child malnutrition burden: stunting and wasting.

Stunting (low height-for-age) and wasting (low weight-for-height) reflect
chronic and acute undernutrition respectively. Both impair cognitive
development, immune function, and adult productivity. Stunting prevalence
above 20% is a public health emergency; above 40% is very high.

Primary indicator: SH.STA.STNT.ZS (stunting % of children under 5).
Fallback: SH.SVR.WAST.ZS (severe wasting % of children under 5).

High score = high malnutrition stress.

References:
    WHO (2021). WHO Child Growth Standards. Geneva.
    Black, R.E. et al. (2013). Maternal and child undernutrition and
        overweight in low-income and middle-income countries. Lancet, 382.

Sources: WDI 'SH.STA.STNT.ZS', fallback 'SH.SVR.WAST.ZS'.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class MalnutritionBurden(LayerBase):
    layer_id = "l8"
    name = "Malnutrition Burden"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Score child malnutrition using stunting or wasting prevalence.

        Formula (stunting): score = clip(stunting_pct * 1.5, 0, 100).
        At 67% stunting the score reaches 100.
        Fallback to wasting if stunting unavailable (wasting * 10, clipped).
        """
        country = kwargs.get("country_iso3", "BGD")

        stunting_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SH.STA.STNT.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        wasting_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SH.SVR.WAST.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        indicator_used = None
        value = None
        year = None

        if stunting_rows:
            row = stunting_rows[0]
            value = float(row["value"])
            year = row["date"][:4]
            indicator_used = "stunting"
            score = float(min(max(value * 1.5, 0.0), 100.0))
        elif wasting_rows:
            row = wasting_rows[0]
            value = float(row["value"])
            year = row["date"][:4]
            indicator_used = "wasting"
            # Wasting prevalence thresholds are smaller (>15% = very high)
            score = float(min(max(value * 6.67, 0.0), 100.0))
        else:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no malnutrition data (SH.STA.STNT.ZS or SH.SVR.WAST.ZS)",
            }

        return {
            "score": score,
            "results": {
                "country": country,
                "year": year,
                "indicator": indicator_used,
                "prevalence_pct": value,
                "who_threshold_high": 20.0 if indicator_used == "stunting" else 5.0,
                "severity": (
                    "very high" if value > 40 else
                    "high" if value > 20 else
                    "moderate" if value > 10 else
                    "low"
                ) if indicator_used == "stunting" else (
                    "very high" if value > 15 else
                    "high" if value > 10 else
                    "moderate" if value > 5 else
                    "low"
                ),
            },
        }
