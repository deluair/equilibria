"""Arts market concentration: cultural market access and diversity.

A concentrated arts market -- dominated by a few global platforms or studios --
limits cultural diversity, restricts local market access, and suppresses
independent creator revenues. Proxied by internet users share (IT.NET.USER.ZS)
as a proxy for digital cultural market access, combined with mobile subscription
penetration (IT.CEL.SETS.P2) as a signal of media/telecom market reach.

Score: low internet penetration -> STABLE (limited digital market), moderate ->
WATCH developing access but concentration risk, high penetration -> STRESS
(dominant platforms may crowd out local culture), saturation -> CRISIS
(potential monopolistic concentration of attention and revenue).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class ArtsMarketConcentration(LayerBase):
    layer_id = "lAR"
    name = "Arts Market Concentration"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        inet_code = "IT.NET.USER.ZS"
        mob_code = "IT.CEL.SETS.P2"

        inet_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (inet_code, "%Individuals using the Internet%"),
        )
        mob_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (mob_code, "%Mobile cellular subscriptions%"),
        )

        inet_vals = [r["value"] for r in inet_rows if r["value"] is not None]
        mob_vals = [r["value"] for r in mob_rows if r["value"] is not None]

        if not inet_vals and not mob_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for IT.NET.USER.ZS or IT.CEL.SETS.P2",
            }

        inet_latest = inet_vals[0] if inet_vals else 0.0
        mob_latest = mob_vals[0] if mob_vals else 0.0

        inet_trend = round(inet_vals[0] - inet_vals[-1], 3) if len(inet_vals) > 1 else None

        # Concentration score: high internet + high mobile = high platform market concentration risk
        # IT.NET.USER.ZS: 0-100% of population
        # IT.CEL.SETS.P2: subscriptions per 100 people (can exceed 100)
        inet_norm = min(100.0, inet_latest)  # already a percentage
        mob_norm = min(100.0, mob_latest)    # cap at 100 for scoring

        # Combined score: 60% internet access, 40% mobile
        score = 0.60 * inet_norm + 0.40 * mob_norm

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "internet_users_pct": round(inet_latest, 2),
                "mobile_subscriptions_per100": round(mob_latest, 2),
                "inet_norm_score": round(inet_norm, 2),
                "mob_norm_score": round(mob_norm, 2),
                "trend_internet_pct": inet_trend,
                "n_obs_internet": len(inet_vals),
                "n_obs_mobile": len(mob_vals),
            },
        }
