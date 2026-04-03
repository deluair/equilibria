"""Crowdfunding economics: digital infrastructure capacity for online giving.

Crowdfunding viability depends on internet penetration and mobile payment
adoption. Proxied via WDI internet users (% of population, IT.NET.USER.ZS)
and mobile cellular subscriptions per 100 people (IT.CEL.SETS.P2) as inputs.
Together they capture digital financial inclusion enabling crowdfunding platforms.

Score: low digital penetration -> CRISIS unable to support crowdfunding;
high digital access -> STABLE enabling environment for online philanthropy.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class CrowdfundingEconomics(LayerBase):
    layer_id = "lNP"
    name = "Crowdfunding Economics"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        inet_code = "IT.NET.USER.ZS"
        mobile_code = "IT.CEL.SETS.P2"

        inet_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (inet_code, "%internet users%"),
        )
        mobile_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (mobile_code, "%mobile cellular subscriptions%"),
        )

        inet_vals = [r["value"] for r in inet_rows if r["value"] is not None]
        mobile_vals = [r["value"] for r in mobile_rows if r["value"] is not None]

        if not inet_vals and not mobile_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for internet or mobile penetration",
            }

        inet = inet_vals[0] if inet_vals else 0.0
        mobile = min(mobile_vals[0], 100.0) if mobile_vals else 0.0
        inet_trend = round(inet_vals[0] - inet_vals[-1], 3) if len(inet_vals) > 1 else None

        # Composite digital capacity: internet (70% weight) + mobile (30%)
        digital_score = inet * 0.70 + mobile * 0.30

        # Invert: higher digital capacity = lower stress (lower score = better)
        if digital_score >= 80:
            score = 10.0 + (100.0 - digital_score) * 0.2
        elif digital_score >= 50:
            score = 20.0 + (80.0 - digital_score) * 0.5
        elif digital_score >= 25:
            score = 35.0 + (50.0 - digital_score) * 0.8
        else:
            score = min(100.0, 55.0 + (25.0 - digital_score) * 1.5)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "internet_users_pct": round(inet, 2),
                "mobile_per_100": round(mobile_vals[0] if mobile_vals else 0.0, 2),
                "digital_capacity_score": round(digital_score, 2),
                "internet_trend_pct": inet_trend,
                "n_obs_internet": len(inet_vals),
                "n_obs_mobile": len(mobile_vals),
            },
        }
