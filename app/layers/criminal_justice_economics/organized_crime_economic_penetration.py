"""Organized crime economic penetration: illicit economy share via shadow economy proxy.

Organized crime infiltrates legitimate economies through money laundering, extortion,
contract fraud, and regulatory capture. The shadow economy size (% of GDP) is the
best available proxy for illicit economic activity, capturing unreported transactions
that overlap with organized crime markets. Schneider & Enste estimate global shadow
economy at 15-20% of GDP on average, with criminal penetration correlated.

Score: small shadow economy (<10% GDP) -> STABLE, moderate (10-20%) -> WATCH,
large (20-35%) -> STRESS, very large (>35%) -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class OrganizedCrimeEconomicPenetration(LayerBase):
    layer_id = "lCJ"
    name = "Organized Crime Economic Penetration"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        # Shadow economy proxy: self-employed % of total employment (higher = more informal/shadow)
        se_code = "SL.EMP.SELF.ZS"
        se_name = "self-employed"

        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (se_code, f"%{se_name}%"),
        )
        vals = [r["value"] for r in rows if r["value"] is not None]

        if not vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for shadow economy proxy SL.EMP.SELF.ZS",
            }

        latest = vals[0]
        trend = round(vals[0] - vals[-1], 3) if len(vals) > 1 else None

        # High self-employment in developing economies correlates with shadow economy
        # and organized crime penetration risk
        if latest < 15:
            score = 5.0 + latest * 0.8
        elif latest < 30:
            score = 17.0 + (latest - 15) * 1.8
        elif latest < 50:
            score = 44.0 + (latest - 30) * 1.3
        else:
            score = min(100.0, 70.0 + (latest - 50) * 0.8)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "self_employed_pct": round(latest, 2),
                "trend": trend,
                "n_obs": len(vals),
                "shadow_economy_risk": (
                    "low" if latest < 15
                    else "moderate" if latest < 30
                    else "high" if latest < 50
                    else "very_high"
                ),
            },
        }
