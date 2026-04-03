"""Disability social protection: disability benefits as share of social protection spending.

Disability benefits -- cash transfers, in-kind services, rehabilitation -- are
a core pillar of inclusive social protection. Proxied by total transfer payments
as a share of government expenditure (GC.XPN.TRFT.ZS). Low transfer spending
signals under-investment in the social protection systems that disabled persons
depend on most.

Score: high transfer spending -> STABLE well-provisioned protection floor.
Very low transfer spending -> CRISIS systemic protection gap.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class DisabilitySocialProtection(LayerBase):
    layer_id = "lDI"
    name = "Disability Social Protection"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "GC.XPN.TRFT.ZS"

        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, "%transfer%"),
        )

        vals = [r["value"] for r in rows if r["value"] is not None]

        if not vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for GC.XPN.TRFT.ZS"}

        transfer_pct = vals[0]
        trend = round(vals[0] - vals[-1], 3) if len(vals) > 1 else None

        # Low transfer spending = higher gap score
        # Reference: 30%+ of gov expenditure = strong; <10% = very weak
        if transfer_pct >= 30:
            score = 10.0
        elif transfer_pct >= 20:
            score = 10.0 + (30.0 - transfer_pct) * 1.5
        elif transfer_pct >= 10:
            score = 25.0 + (20.0 - transfer_pct) * 2.5
        else:
            score = min(100.0, 50.0 + (10.0 - transfer_pct) * 5.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "transfer_expenditure_pct": round(transfer_pct, 2),
                "trend": trend,
                "protection_tier": (
                    "strong" if transfer_pct >= 30
                    else "moderate" if transfer_pct >= 20
                    else "weak" if transfer_pct >= 10
                    else "critical_gap"
                ),
                "n_obs": len(vals),
            },
        }
