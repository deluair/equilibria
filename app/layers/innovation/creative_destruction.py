"""Creative Destruction module.

Measures Schumpeterian creative destruction dynamics using:
- New business density (new registrations per 1,000 people aged 15-64)
  (IC.BUS.NDNS.ZS) -- business entry rate proxy
- Manufacturing value added as % of GDP (NV.IND.MANF.ZS) -- structural change proxy

Low business entry combined with declining manufacturing value added suggests
low creative destruction: the economy is not generating new firms or reallocating
resources from declining sectors. This indicates stagnant structural change and
weak entrepreneurial dynamism.

Score: higher = lower creative destruction (more stress).

Sources: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CreativeDestruction(LayerBase):
    layer_id = "lNV"
    name = "Creative Destruction"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        entry_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IC.BUS.NDNS.ZS'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        manuf_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NV.IND.MANF.ZS'
              AND dp.value IS NOT NULL
            ORDER BY dp.date ASC
            """,
            (country,),
        )

        entry: float | None = None
        manuf_trend: float | None = None
        manuf_current: float | None = None

        if entry_rows:
            vals = [float(r["value"]) for r in entry_rows if r["value"] is not None]
            entry = float(np.mean(vals)) if vals else None

        if manuf_rows and len(manuf_rows) >= 2:
            manuf_vals = np.array([float(r["value"]) for r in manuf_rows if r["value"] is not None])
            manuf_current = float(manuf_vals[-1])
            # OLS trend coefficient (slope)
            n = len(manuf_vals)
            t = np.arange(n, dtype=float)
            slope = float(np.polyfit(t, manuf_vals, 1)[0])
            manuf_trend = slope
        elif manuf_rows:
            manuf_current = float(manuf_rows[0]["value"])

        if entry is None and manuf_trend is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        # Score components
        score_parts: list[float] = []

        if entry is not None:
            # New business density: 0-10 per 1,000 -> 0-100 composite
            # Low entry = higher stress
            entry_norm = min(100.0, (entry / 10.0) * 100.0)
            entry_score = max(0.0, 100.0 - entry_norm)
            score_parts.append(entry_score)

        if manuf_trend is not None:
            # Declining manufacturing (negative trend) adds stress
            # Trend range roughly -2 to +2 percentage points/year
            trend_score = min(50.0, max(0.0, (-manuf_trend / 2.0) * 50.0))
            score_parts.append(trend_score)

        score = float(np.mean(score_parts))

        return {
            "score": round(score, 1),
            "country": country,
            "new_business_density": round(entry, 4) if entry is not None else None,
            "manufacturing_pct_gdp": round(manuf_current, 2) if manuf_current is not None else None,
            "manufacturing_trend_pp_yr": round(manuf_trend, 4) if manuf_trend is not None else None,
            "interpretation": (
                "High score = slow entry + declining manufacturing = low creative destruction"
            ),
        }
