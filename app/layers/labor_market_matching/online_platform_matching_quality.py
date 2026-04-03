"""Online platform matching quality: internet penetration + digital literacy as online job matching capacity.

Digital job platforms (LinkedIn, Indeed, local equivalents) have substantially
reduced search frictions by aggregating job postings and enabling rapid matching
at scale. Internet penetration and digital literacy determine how broadly the
population can access these platforms. High coverage = low online matching
barriers; low coverage = geographic and informational friction persists.

Score: high internet penetration (>80%) -> STABLE broad platform access,
moderate (50-80%) -> WATCH partial coverage, low (20-50%) -> STRESS
significant digital exclusion, very low (<20%) -> CRISIS analog-only market.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class OnlinePlatformMatchingQuality(LayerBase):
    layer_id = "lLM"
    name = "Online Platform Matching Quality"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        internet_code = "IT.NET.USER.ZS"
        mobile_code = "IT.CEL.SETS.P2"

        internet_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (internet_code, "%internet%users%"),
        )
        mobile_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (mobile_code, "%mobile cellular%"),
        )

        internet_vals = [r["value"] for r in internet_rows if r["value"] is not None]
        mobile_vals = [r["value"] for r in mobile_rows if r["value"] is not None]

        if not internet_vals and not mobile_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for internet penetration IT.NET.USER.ZS",
            }

        if internet_vals:
            penetration = internet_vals[0]
            trend = round(internet_vals[0] - internet_vals[-1], 3) if len(internet_vals) > 1 else None
        else:
            # Mobile subscriptions per 100 as fallback proxy
            mobile_rate = min(100.0, mobile_vals[0] / 1.5)  # rough conversion
            penetration = mobile_rate
            trend = None

        # Invert: higher penetration = lower matching friction = lower score
        if penetration >= 80:
            score = max(0.0, (100.0 - penetration) * 0.5)
        elif penetration >= 50:
            score = 10.0 + (80.0 - penetration) * 0.8
        elif penetration >= 20:
            score = 34.0 + (50.0 - penetration) * 1.2
        else:
            score = min(100.0, 70.0 + (20.0 - penetration) * 1.5)

        # Trend adjustment: rapidly growing internet access lowers future friction
        if trend is not None:
            if trend > 10:
                score = max(0.0, score - 8.0)
            elif trend > 5:
                score = max(0.0, score - 4.0)
            elif trend < 0:
                score = min(100.0, score + 5.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "internet_penetration_pct": round(penetration, 2),
                "mobile_subscriptions_per100": round(mobile_vals[0], 2) if mobile_vals else None,
                "internet_growth_trend_pct": trend,
                "n_obs_internet": len(internet_vals),
                "n_obs_mobile": len(mobile_vals),
                "platform_access": (
                    "broad" if penetration >= 80
                    else "moderate" if penetration >= 50
                    else "limited" if penetration >= 20
                    else "minimal"
                ),
            },
        }
