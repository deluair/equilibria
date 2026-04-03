"""Tourism Multiplier Effect module.

Estimates the multiplier effect of tourism by comparing growth in tourism
receipts (ST.INT.RCPT.XP.ZS) with services employment share (SL.SRV.EMPL.ZS).
A strong positive association suggests effective spillover from tourism
spending into broader service-sector employment.

Score: 0 (strong multiplier) to 100 (weak or absent multiplier).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class TourismMultiplierEffect(LayerBase):
    layer_id = "lTO"
    name = "Tourism Multiplier Effect"

    async def compute(self, db, **kwargs) -> dict:
        receipts_code = "ST.INT.RCPT.XP.ZS"
        empl_code = "SL.SRV.EMPL.ZS"

        receipts_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (receipts_code, "%tourism receipts%"),
        )

        empl_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (empl_code, "%services.*employment%"),
        )

        receipts_vals = [float(r["value"]) for r in receipts_rows if r["value"] is not None]
        empl_vals = [float(r["value"]) for r in empl_rows if r["value"] is not None]

        if not receipts_vals and not empl_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for ST.INT.RCPT.XP.ZS or SL.SRV.EMPL.ZS",
            }

        # If only one series available, score based on that alone
        if not receipts_vals or not empl_vals:
            available = receipts_vals or empl_vals
            latest = available[0]
            # Moderate score if data is partial
            return {
                "score": 50.0,
                "signal": "WATCH",
                "tourism_receipts_pct": round(receipts_vals[0], 2) if receipts_vals else None,
                "services_employment_pct": round(empl_vals[0], 2) if empl_vals else None,
                "note": "partial data; multiplier estimate unreliable",
                "n_receipts_obs": len(receipts_vals),
                "n_empl_obs": len(empl_vals),
            }

        # Use minimum common length for comparison
        n = min(len(receipts_vals), len(empl_vals))
        r_arr = np.array(receipts_vals[:n][::-1])
        e_arr = np.array(empl_vals[:n][::-1])

        # Correlation between tourism share growth and services employment
        if n >= 3:
            r_diff = np.diff(r_arr)
            e_diff = np.diff(e_arr)
            if np.std(r_diff) > 0 and np.std(e_diff) > 0:
                corr = float(np.corrcoef(r_diff, e_diff)[0, 1])
            else:
                corr = 0.0
        else:
            corr = 0.0

        latest_receipts = receipts_vals[0]
        latest_empl = empl_vals[0]

        # Strong positive correlation = strong multiplier = low score
        # No/negative correlation = weak multiplier = high score
        # Map corr -1 -> 85, 0 -> 50, 1 -> 15
        score = float(np.clip(50 - corr * 35, 10, 90))

        return {
            "score": round(score, 1),
            "tourism_receipts_pct": round(latest_receipts, 2),
            "services_employment_pct": round(latest_empl, 2),
            "receipts_empl_correlation": round(corr, 3),
            "n_obs": n,
            "methodology": "score = clip(50 - corr * 35, 10, 90); corr of tourism receipts vs services empl changes",
        }
