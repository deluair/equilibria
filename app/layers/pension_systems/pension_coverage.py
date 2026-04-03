"""Pension Coverage module.

Estimates pension system coverage gaps by combining elderly population share
with social protection transfer levels.

Low social transfers for an aging population imply large segments of the elderly
are not adequately covered by formal pension arrangements.

Score = clip(elderly_share * max(0, 20 - transfers) / 10, 0, 100)

Sources: WDI SP.POP.65UP.TO.ZS (elderly % of total population),
         GC.XPN.TRFT.ZS (social protection transfers % of expense)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PensionCoverage(LayerBase):
    layer_id = "lPS"
    name = "Pension Coverage"

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

        if transfer_rows:
            transfer_vals = [float(r["value"]) for r in transfer_rows if r["value"] is not None]
            transfers = float(np.mean(transfer_vals)) if transfer_vals else 5.0
        else:
            transfers = 5.0  # fallback: assume minimal transfers

        score = float(np.clip(elderly_share * max(0.0, 20.0 - transfers) / 10.0, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "elderly_share_pct": round(elderly_share, 2),
            "social_transfers_pct_expense": round(transfers, 2),
            "coverage_gap": transfers < 10.0,
            "interpretation": (
                "severe coverage gap" if score > 75
                else "moderate coverage gap" if score > 50
                else "limited coverage gap" if score > 25
                else "adequate coverage"
            ),
            "sources": ["WDI SP.POP.65UP.TO.ZS", "WDI GC.XPN.TRFT.ZS"],
        }
