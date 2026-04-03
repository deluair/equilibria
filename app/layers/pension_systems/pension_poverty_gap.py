"""Pension Poverty Gap module.

Estimates elderly-specific poverty risk as a composite of old-age dependency
ratio, general poverty headcount, and inadequacy of social protection
transfers. Countries with high elderly populations, high poverty, and low
transfers face the largest pension poverty gaps.

Score = clip((elderly_share * poverty_rate / 10) + max(0, 30 - transfers), 0, 100)

Sources: WDI SP.POP.65UP.TO.ZS (elderly % of total population),
         WDI SI.POV.DDAY (poverty headcount ratio at $2.15/day, % of population),
         WDI GC.XPN.TRFT.ZS (social transfers % of expense)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PensionPovertyGap(LayerBase):
    layer_id = "lPS"
    name = "Pension Poverty Gap"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        elderly_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SP.POP.65UP.TO.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        poverty_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SI.POV.DDAY'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        transfer_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.XPN.TRFT.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not elderly_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no elderly population data"}

        elderly_vals = [float(r["value"]) for r in elderly_rows if r["value"] is not None]
        if not elderly_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid elderly data"}

        elderly_share = float(np.mean(elderly_vals))

        poverty_vals = [float(r["value"]) for r in poverty_rows if r["value"] is not None]
        poverty_rate = float(np.mean(poverty_vals)) if poverty_vals else 10.0

        transfer_vals = [float(r["value"]) for r in transfer_rows if r["value"] is not None]
        transfers = float(np.mean(transfer_vals)) if transfer_vals else 5.0

        composite = elderly_share * poverty_rate / 10.0 + max(0.0, 30.0 - transfers)
        score = float(np.clip(composite, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "elderly_share_pct": round(elderly_share, 2),
            "poverty_rate_pct": round(poverty_rate, 2),
            "social_transfers_pct_expense": round(transfers, 2),
            "composite_raw": round(composite, 3),
            "high_elderly_poverty_risk": score > 50,
            "interpretation": (
                "critical elderly poverty gap" if score > 75
                else "high elderly poverty risk" if score > 50
                else "moderate elderly poverty risk" if score > 25
                else "low elderly poverty risk"
            ),
            "sources": [
                "WDI SP.POP.65UP.TO.ZS",
                "WDI SI.POV.DDAY",
                "WDI GC.XPN.TRFT.ZS",
            ],
        }
