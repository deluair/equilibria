"""Health system capacity: hospital beds and physician density.

Health system capacity is proxied by two infrastructure metrics:
  - Hospital beds per 1,000 population (SH.MED.BEDS.ZS)
  - Physicians per 1,000 population (SH.MED.PHYS.ZS)

Low values on both dimensions indicate a health system ill-equipped to
manage routine care, emergencies, or epidemic surges. WHO recommends at
least 4 beds per 1,000 and 1 physician per 1,000 as minimum thresholds.

High score = low capacity = high stress.

Scoring:
  beds_stress   = max(0, 4 - beds) * 15   [max 60 if beds = 0]
  phys_stress   = max(0, 1.5 - phys) * 20 [max 30 if phys = 0]
  raw_score     = beds_stress + phys_stress
  score         = clip(raw_score, 0, 100)

References:
    WHO (2016). Minimum Data Set for Health System Capacity. Geneva.
    Kruk, M.E. et al. (2018). High-quality health systems in the SDG era.
        Lancet Global Health, 6(11), e1196-e1252.

Sources: WDI 'SH.MED.BEDS.ZS', 'SH.MED.PHYS.ZS'.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class HealthSystemCapacity(LayerBase):
    layer_id = "l8"
    name = "Health System Capacity"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Score health system capacity from bed and physician density.

        Uses the most recent available year for each indicator independently,
        since they are often reported in different years.
        """
        country = kwargs.get("country_iso3", "BGD")

        beds_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SH.MED.BEDS.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        phys_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SH.MED.PHYS.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not beds_rows and not phys_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no capacity data (SH.MED.BEDS.ZS or SH.MED.PHYS.ZS)",
            }

        beds = float(beds_rows[0]["value"]) if beds_rows else None
        beds_year = beds_rows[0]["date"][:4] if beds_rows else None
        physicians = float(phys_rows[0]["value"]) if phys_rows else None
        phys_year = phys_rows[0]["date"][:4] if phys_rows else None

        # Compute sub-scores; use benchmark midpoints if one indicator missing
        beds_stress = max(0.0, 4.0 - (beds if beds is not None else 2.0)) * 15.0
        phys_stress = max(0.0, 1.5 - (physicians if physicians is not None else 0.75)) * 20.0

        score = float(min(beds_stress + phys_stress, 100.0))

        return {
            "score": score,
            "results": {
                "country": country,
                "hospital_beds_per_1000": beds,
                "beds_year": beds_year,
                "physicians_per_1000": physicians,
                "physicians_year": phys_year,
                "who_beds_threshold": 4.0,
                "who_physicians_threshold": 1.0,
                "beds_adequate": (beds >= 4.0) if beds is not None else None,
                "physicians_adequate": (physicians >= 1.0) if physicians is not None else None,
            },
        }
