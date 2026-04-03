"""Informal Economy Protection module.

Informal workers excluded from social protection:
self-employment rate combined with low social transfers.

Queries:
- 'SL.EMP.SELF.ZS' (self-employed workers as % of total employment)
- 'GC.XPN.TRFT.ZS' (social transfers as % of government expenditure)

High informal (self-employed) share + low social transfers = protection exclusion gap.

Score = clip(self_employment * max(0, 30 - transfer_share) / 15, 0, 100)

Sources: WDI (SL.EMP.SELF.ZS, GC.XPN.TRFT.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class InformalEconomyProtection(LayerBase):
    layer_id = "lSP"
    name = "Informal Economy Protection"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        self_emp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SL.EMP.SELF.ZS'
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

        if not self_emp_rows or not transfer_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        self_emp_vals = [float(r["value"]) for r in self_emp_rows if r["value"] is not None]
        transfer_vals = [float(r["value"]) for r in transfer_rows if r["value"] is not None]

        if not self_emp_vals or not transfer_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        self_employment = float(np.mean(self_emp_vals))
        transfer_share = float(np.mean(transfer_vals))

        transfer_gap = max(0.0, 30.0 - transfer_share)
        score = float(np.clip(self_employment * transfer_gap / 15.0, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "self_employment_pct": round(self_employment, 2),
            "social_transfers_pct_expenditure": round(transfer_share, 2),
            "transfer_gap": round(transfer_gap, 2),
            "n_obs_self_emp": len(self_emp_vals),
            "n_obs_transfers": len(transfer_vals),
            "interpretation": (
                "High self-employment (informal work proxy) combined with low social "
                "transfers indicates informal workers are excluded from social protection."
            ),
            "_series": ["SL.EMP.SELF.ZS", "GC.XPN.TRFT.ZS"],
            "_source": "WDI",
        }
