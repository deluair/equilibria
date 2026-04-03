"""Gender education gap module.

Measures parity in school enrollment across primary, secondary, and tertiary
levels using the Gender Parity Index (GPI = female/male gross enrollment ratio).

GPI = 1.0 -> perfect parity (score = 0).
GPI < 1.0 -> girls lag (gap = 1 - GPI).
GPI > 1.0 -> boys lag (gap = GPI - 1).

Both deviations from 1.0 signal inequality; we take abs(1 - GPI).

Scoring (using secondary GPI as primary signal):
    dev = abs(1.0 - gpi_secondary)
    score = clip(dev * 200, 0, 100)

    dev = 0.00 -> score = 0   (parity)
    dev = 0.12 -> score = 25  (watch)
    dev = 0.25 -> score = 50  (stress)
    dev = 0.38 -> score = 75
    dev = 0.50 -> score = 100 (crisis)

Sources: WDI (SE.ENR.PRSC.FM.ZS primary, SE.ENR.SECO.FM.ZS secondary,
SE.ENR.TERT.FM.ZS tertiary GPI).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

SERIES_PRI = "SE.ENR.PRSC.FM.ZS"
SERIES_SEC = "SE.ENR.SECO.FM.ZS"
SERIES_TER = "SE.ENR.TERT.FM.ZS"


class GenderEducationGap(LayerBase):
    layer_id = "lGE"
    name = "Gender Education Gap"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN (
                  'SE.ENR.PRSC.FM.ZS',
                  'SE.ENR.SECO.FM.ZS',
                  'SE.ENR.TERT.FM.ZS'
              )
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no enrollment GPI data for gender education gap",
            }

        def latest(series_id: str) -> float | None:
            filtered = [r for r in rows if r["series_id"] == series_id]
            return float(filtered[0]["value"]) if filtered else None

        gpi_primary = latest(SERIES_PRI)
        gpi_secondary = latest(SERIES_SEC)
        gpi_tertiary = latest(SERIES_TER)

        # Use secondary as scoring anchor; fall back to primary
        anchor = gpi_secondary if gpi_secondary is not None else gpi_primary
        if anchor is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "missing secondary and primary GPI data",
            }

        dev = abs(1.0 - anchor)
        score = float(np.clip(dev * 200.0, 0.0, 100.0))

        sec_rows = [r for r in rows if r["series_id"] == SERIES_SEC]
        latest_date = sec_rows[0]["date"] if sec_rows else rows[0]["date"]

        return {
            "score": round(score, 2),
            "country": country,
            "gpi_primary": round(gpi_primary, 4) if gpi_primary is not None else None,
            "gpi_secondary": round(gpi_secondary, 4) if gpi_secondary is not None else None,
            "gpi_tertiary": round(gpi_tertiary, 4) if gpi_tertiary is not None else None,
            "scoring_anchor": "secondary" if gpi_secondary is not None else "primary",
            "deviation_from_parity": round(dev, 4),
            "latest_date": latest_date,
            "note": "score = clip(abs(1 - GPI_secondary) * 200, 0, 100). GPI < 1 = girls lag, > 1 = boys lag",
        }
