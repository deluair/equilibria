"""Caregiver economic burden: informal care economic cost.

Informal caregiving -- primarily provided by women -- represents a massive
unmeasured economic contribution and a direct constraint on female labor force
participation. Low female labor force participation (SL.TLF.CACT.FE.ZS) relative
to the full participation benchmark signals that caregiving responsibilities are
drawing women out of formal employment, imposing real economic costs on households
and the broader economy.

Score: high female LFPR -> STABLE low caregiver burden.
Very low female LFPR -> CRISIS high caregiver burden crowding out formal work.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class CaregiverEconomicBurden(LayerBase):
    layer_id = "lDI"
    name = "Caregiver Economic Burden"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        lfpr_code = "SL.TLF.CACT.FE.ZS"

        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (lfpr_code, "%female labor force%"),
        )

        vals = [r["value"] for r in rows if r["value"] is not None]

        if not vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for SL.TLF.CACT.FE.ZS"}

        female_lfpr = vals[0]
        trend = round(vals[0] - vals[-1], 3) if len(vals) > 1 else None

        # Low female LFPR = higher caregiver burden (inverse relationship)
        # Reference: 70%+ = high participation (low burden signal)
        # <30% = very low participation (high caregiver burden)
        if female_lfpr >= 70:
            score = 8.0
        elif female_lfpr >= 55:
            score = 8.0 + (70.0 - female_lfpr) * 1.13
        elif female_lfpr >= 40:
            score = 25.0 + (55.0 - female_lfpr) * 1.67
        else:
            score = min(100.0, 50.0 + (40.0 - female_lfpr) * 1.25)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "female_lfpr_pct": round(female_lfpr, 2),
                "trend": trend,
                "burden_tier": (
                    "low" if female_lfpr >= 70
                    else "moderate" if female_lfpr >= 55
                    else "high" if female_lfpr >= 40
                    else "severe"
                ),
                "n_obs": len(vals),
            },
        }
