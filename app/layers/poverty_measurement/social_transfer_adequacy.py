"""Social Transfer Adequacy module.

Assesses whether government social transfers (GC.XPN.TRFT.ZS -- transfers and
subsidies as % of expense) are sufficient relative to the depth of poverty
(SI.POV.GAPS). Adequacy is defined as the ratio of transfer spending to the
poverty gap: higher ratios indicate that fiscal resources are better targeted
at closing the poverty gap.

Score = clip(100 - adequacy_ratio * 10, 0, 100)  -- low adequacy = high stress.

Sources: WDI (GC.XPN.TRFT.ZS, SI.POV.GAPS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class SocialTransferAdequacy(LayerBase):
    layer_id = "lPM"
    name = "Social Transfer Adequacy"

    async def compute(self, db, **kwargs) -> dict:
        transfer_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("GC.XPN.TRFT.ZS", "%transfers%subsidies%expense%"),
        )
        gap_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("SI.POV.GAPS", "%poverty gap%"),
        )

        transfer_vals = [float(r["value"]) for r in transfer_rows if r["value"] is not None] if transfer_rows else []
        gap_vals = [float(r["value"]) for r in gap_rows if r["value"] is not None] if gap_rows else []

        if not transfer_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for GC.XPN.TRFT.ZS"}

        transfer_pct = transfer_vals[0]
        gap_pct = gap_vals[0] if gap_vals else None

        adequacy_ratio = None
        if gap_pct is not None and gap_pct > 0:
            adequacy_ratio = transfer_pct / gap_pct

        if adequacy_ratio is not None:
            score = float(np.clip(100 - adequacy_ratio * 10, 0, 100))
        else:
            # No gap data: score from transfer level alone -- low transfers = stress
            score = float(np.clip(100 - transfer_pct * 2, 0, 100))

        return {
            "score": round(score, 1),
            "transfer_pct_expense": round(transfer_pct, 2),
            "poverty_gap_pct": round(gap_pct, 3) if gap_pct is not None else None,
            "adequacy_ratio": round(adequacy_ratio, 3) if adequacy_ratio is not None else None,
            "n_obs_transfers": len(transfer_vals),
            "n_obs_gap": len(gap_vals),
            "interpretation": "transfer_pct / gap_pct; higher ratio = more adequate coverage",
        }
