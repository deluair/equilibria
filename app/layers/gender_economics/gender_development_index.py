"""Gender Development Index module.

The UNDP Gender Development Index (GDI) measures gender inequalities in
human development achievements across three dimensions: health (life expectancy),
education (mean and expected years of schooling), and income (GNI per capita).

GDI = HDI_female / HDI_male

GDI < 1.0 means women have lower human development.
GDI = 1.0 means parity.
GDI > 1.0 means women have higher human development (rare, context-dependent).

We measure deviation from parity, penalizing gaps in both directions:
    dev = abs(1.0 - gdi)
    score = clip(dev * 200, 0, 100)

    GDI = 1.00 -> score = 0   (parity)
    GDI = 0.88 -> score = 25  (watch)
    GDI = 0.75 -> score = 50  (stress)
    GDI = 0.63 -> score = 75
    GDI = 0.50 -> score = 100 (crisis)

Sources: UNDP HDR (GDI stored as 'HDR.GDI' series).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

SERIES = "HDR.GDI"
# Component series for detailed decomposition
SERIES_HDI_F = "HDR.HDI.FE"
SERIES_HDI_M = "HDR.HDI.MA"


class GenderDevelopmentIndex(LayerBase):
    layer_id = "lGE"
    name = "Gender Development Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('HDR.GDI', 'HDR.HDI.FE', 'HDR.HDI.MA')
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no GDI data (HDR.GDI, HDR.HDI.FE, HDR.HDI.MA)",
            }

        def latest(series_id: str) -> float | None:
            filtered = [r for r in rows if r["series_id"] == series_id]
            return float(filtered[0]["value"]) if filtered else None

        def latest_date(series_id: str) -> str | None:
            filtered = [r for r in rows if r["series_id"] == series_id]
            return filtered[0]["date"] if filtered else None

        gdi = latest(SERIES)

        # If direct GDI is unavailable, compute from component HDI values
        if gdi is None:
            hdi_f = latest(SERIES_HDI_F)
            hdi_m = latest(SERIES_HDI_M)
            if hdi_f is not None and hdi_m is not None and hdi_m > 0:
                gdi = hdi_f / hdi_m
            else:
                return {
                    "score": None,
                    "signal": "UNAVAILABLE",
                    "error": "no GDI or HDI component data available",
                }

        if gdi <= 0:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "invalid GDI value",
            }

        dev = abs(1.0 - gdi)
        score = float(np.clip(dev * 200.0, 0.0, 100.0))

        # Classify GDI group (UNDP groups: 1=high, 2=medium-high, 3=medium, 4=medium-low, 5=low)
        if abs(1.0 - gdi) <= 0.01:
            gdi_group = "group1_high_equality"
        elif abs(1.0 - gdi) <= 0.025:
            gdi_group = "group2_medium_high_equality"
        elif abs(1.0 - gdi) <= 0.05:
            gdi_group = "group3_medium_equality"
        elif abs(1.0 - gdi) <= 0.10:
            gdi_group = "group4_medium_low_equality"
        else:
            gdi_group = "group5_low_equality"

        date_used = latest_date(SERIES) or latest_date(SERIES_HDI_F) or rows[0]["date"]

        return {
            "score": round(score, 2),
            "country": country,
            "gdi": round(gdi, 4),
            "deviation_from_parity": round(dev, 4),
            "gdi_group": gdi_group,
            "hdi_female": latest(SERIES_HDI_F),
            "hdi_male": latest(SERIES_HDI_M),
            "latest_date": date_used,
            "note": "score = clip(abs(1 - GDI) * 200, 0, 100). GDI = HDI_female / HDI_male. Series: HDR.GDI",
        }
