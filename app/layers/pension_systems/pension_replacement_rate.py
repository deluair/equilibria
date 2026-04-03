"""Pension Replacement Rate module.

Proxies pension replacement rate adequacy using social transfer expenditure
levels relative to the working-age population's contribution capacity (labor
force participation). Low transfers relative to an aging population signal
inadequate replacement rates for retirees.

Score = clip(max(0, 30 - transfers) * elderly_share / 5, 0, 100)

Sources: WDI GC.XPN.TRFT.ZS (social transfers % of expense),
         WDI SL.TLF.CACT.ZS (labor force participation rate),
         WDI SP.POP.65UP.TO.ZS (elderly %)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PensionReplacementRate(LayerBase):
    layer_id = "lPS"
    name = "Pension Replacement Rate"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

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

        lfp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SL.TLF.CACT.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

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

        if not transfer_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no social transfer data"}

        transfer_vals = [float(r["value"]) for r in transfer_rows if r["value"] is not None]
        if not transfer_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid transfer data"}

        transfers = float(np.mean(transfer_vals))

        lfp_vals = [float(r["value"]) for r in lfp_rows if r["value"] is not None]
        lfp = float(np.mean(lfp_vals)) if lfp_vals else 60.0

        elderly_vals = [float(r["value"]) for r in elderly_rows if r["value"] is not None]
        elderly_share = float(np.mean(elderly_vals)) if elderly_vals else 7.0

        score = float(np.clip(max(0.0, 30.0 - transfers) * elderly_share / 5.0, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "social_transfers_pct_expense": round(transfers, 2),
            "labor_force_participation_pct": round(lfp, 2),
            "elderly_share_pct": round(elderly_share, 2),
            "transfer_adequacy_gap": round(max(0.0, 30.0 - transfers), 2),
            "inadequate_replacement": transfers < 15.0 and elderly_share > 10.0,
            "interpretation": (
                "critically inadequate replacement rate" if score > 75
                else "inadequate replacement rate" if score > 50
                else "borderline replacement adequacy" if score > 25
                else "adequate replacement rate"
            ),
            "sources": ["WDI GC.XPN.TRFT.ZS", "WDI SL.TLF.CACT.ZS", "WDI SP.POP.65UP.TO.ZS"],
        }
