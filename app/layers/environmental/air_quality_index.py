"""Air quality index: composite of PM2.5 exposure and CO2 per capita.

Queries:
  EN.ATM.PM25.MC.M3  -- PM2.5 air pollution (mean annual exposure, ug/m3)
  EN.ATM.CO2E.PC     -- CO2 emissions (metric tonnes per capita)

Combines both indicators to produce a composite stress score:

  Score = clip((PM25 / 35 + CO2 / 10) * 50, 0, 100)

Benchmarks:
  - WHO IT1 PM2.5 threshold: 35 ug/m3
  - CO2 reference: 10 t/capita (approximate OECD high-income average)
  - A country at PM2.5 = 35 and CO2 = 10 receives a score of 100 (max stress)

Sources: World Bank WDI (EN.ATM.PM25.MC.M3, EN.ATM.CO2E.PC)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class AirQualityIndex(LayerBase):
    layer_id = "l9"
    name = "Air Quality Index"
    weight = 0.20

    # WHO Interim Target 1 PM2.5 annual mean (most lenient interim threshold)
    WHO_IT1_PM25 = 35.0
    # CO2 reference level (metric tonnes per capita)
    CO2_REFERENCE = 10.0

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3")

        if not country:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "country_iso3 required",
            }

        pm25_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'EN.ATM.PM25.MC.M3'
              AND ds.country_iso3 = ?
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        co2_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'EN.ATM.CO2E.PC'
              AND ds.country_iso3 = ?
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        def _latest(rows):
            for r in rows:
                if r["value"] is not None:
                    return float(r["value"]), r["date"][:4]
            return None, None

        pm25_val, pm25_yr = _latest(pm25_rows)
        co2_val, co2_yr = _latest(co2_rows)

        if pm25_val is None and co2_val is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no PM2.5 or CO2 data available",
            }

        # Partial computation if only one indicator available
        pm25_term = (pm25_val / self.WHO_IT1_PM25) if pm25_val is not None else 0.5
        co2_term = (co2_val / self.CO2_REFERENCE) if co2_val is not None else 0.5

        score = float(np.clip((pm25_term + co2_term) * 50, 0, 100))

        return {
            "score": round(score, 2),
            "results": {
                "country_iso3": country,
                "pm25_ugm3": round(pm25_val, 2) if pm25_val is not None else None,
                "pm25_year": pm25_yr,
                "who_it1_pm25": self.WHO_IT1_PM25,
                "pm25_exceeds_who_it1": (pm25_val > self.WHO_IT1_PM25) if pm25_val is not None else None,
                "co2_per_capita_tonnes": round(co2_val, 2) if co2_val is not None else None,
                "co2_year": co2_yr,
                "co2_reference": self.CO2_REFERENCE,
                "pm25_term": round(pm25_term, 3),
                "co2_term": round(co2_term, 3),
            },
        }
