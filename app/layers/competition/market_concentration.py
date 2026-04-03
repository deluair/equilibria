"""Market Concentration module.

Measures industry concentration using a Herfindahl-Hirschman Index (HHI)
proxy constructed from sectoral GDP shares.

HHI = sum(s_i^2) where s_i is the share of sector i in total GDP.
A perfectly diversified economy (equal shares) minimises HHI.
A monoculture economy maximises it.

Interpretation:
- HHI < 0.15  -> unconcentrated (competitive, diversified)
- 0.15-0.25   -> moderately concentrated
- HHI > 0.25  -> highly concentrated (potential market power concerns)

Score = clip(hhi * 100, 0, 100). High score = high stress (concentrated economy).

Sources: WDI (NV.AGR.TOTL.ZS, NV.IND.TOTL.ZS, NV.SRV.TOTL.ZS, NV.IND.MANF.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_SECTOR_SERIES = [
    "NV.AGR.TOTL.ZS",
    "NV.IND.TOTL.ZS",
    "NV.SRV.TOTL.ZS",
    "NV.IND.MANF.ZS",
]


class MarketConcentration(LayerBase):
    layer_id = "lCO"
    name = "Market Concentration"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        placeholders = ", ".join("?" * len(_SECTOR_SERIES))
        rows = await db.fetch_all(
            f"""
            SELECT ds.series_id, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ({placeholders})
            ORDER BY ds.series_id, dp.date DESC
            """,
            (country, *_SECTOR_SERIES),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no sectoral GDP data"}

        # For each series, take the most recent non-null value
        latest: dict[str, float] = {}
        for r in rows:
            sid = r["series_id"]
            if sid not in latest and r["value"] is not None:
                try:
                    latest[sid] = float(r["value"])
                except (TypeError, ValueError):
                    pass

        if len(latest) < 2:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient sector data"}

        shares_raw = list(latest.values())

        # Normalise to fractions (WDI reports % of GDP)
        total = sum(shares_raw)
        if total <= 0:
            return {"score": None, "signal": "UNAVAILABLE", "error": "invalid sector shares"}

        shares = [s / total for s in shares_raw]

        hhi = float(np.sum(np.array(shares) ** 2))
        score = float(np.clip(hhi * 100, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "hhi": round(hhi, 4),
            "sector_shares": {
                sid: round(v / total, 4) for sid, v in latest.items()
            },
            "n_sectors": len(latest),
            "interpretation": (
                "unconcentrated" if hhi < 0.15
                else "moderately concentrated" if hhi < 0.25
                else "highly concentrated"
            ),
            "reference": "Herfindahl-Hirschman Index; DOJ threshold 0.25 for high concentration",
        }
