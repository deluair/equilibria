"""Assistive technology market: medical device and health technology market size.

Assistive technology -- hearing aids, wheelchairs, screen readers, prosthetics --
is a multi-billion-dollar market that expands with health investment and digital
penetration. Proxied by current health expenditure as a share of GDP
(SH.XPD.CHEX.GD.ZS) and internet user penetration (IT.NET.USER.ZS), which
captures digital assistive technology adoption capacity.

Score: high health spend + high internet penetration -> STABLE large/mature market.
Low health spend + low digital penetration -> CRISIS underserved demand.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class AssistiveTechnologyMarket(LayerBase):
    layer_id = "lDI"
    name = "Assistive Technology Market"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        health_code = "SH.XPD.CHEX.GD.ZS"
        net_code = "IT.NET.USER.ZS"

        health_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (health_code, "%health expenditure%"),
        )
        net_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (net_code, "%internet%"),
        )

        health_vals = [r["value"] for r in health_rows if r["value"] is not None]
        net_vals = [r["value"] for r in net_rows if r["value"] is not None]

        if not health_vals and not net_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for health expenditure or internet users"}

        if health_vals and net_vals:
            health_gdp = health_vals[0]
            internet_pct = net_vals[0]
            # Market readiness: high spend + high connectivity = large/accessible market
            # We invert: low penetration = higher score (larger gap = more stress)
            health_norm = min(1.0, health_gdp / 12.0)  # ~12% GDP is high
            net_norm = min(1.0, internet_pct / 100.0)
            # Gap score: lower health + lower internet = worse (underserved market)
            gap = 1.0 - (health_norm * 0.5 + net_norm * 0.5)
            score = round(gap * 100.0, 2)
            return {
                "score": score,
                "signal": self.classify_signal(score),
                "metrics": {
                    "health_expenditure_gdp_pct": round(health_gdp, 2),
                    "internet_users_pct": round(internet_pct, 2),
                    "market_gap_index": round(gap, 4),
                    "n_obs_health": len(health_vals),
                    "n_obs_internet": len(net_vals),
                },
            }

        if health_vals:
            health_gdp = health_vals[0]
            # Low health spend = high market gap
            gap = max(0.0, 1.0 - health_gdp / 12.0)
            score = round(gap * 100.0, 2)
            return {
                "score": score,
                "signal": self.classify_signal(score),
                "metrics": {
                    "health_expenditure_gdp_pct": round(health_gdp, 2),
                    "internet_users_pct": None,
                    "n_obs_health": len(health_vals),
                },
            }

        internet_pct = net_vals[0]
        gap = max(0.0, 1.0 - internet_pct / 100.0)
        score = round(gap * 100.0, 2)
        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "health_expenditure_gdp_pct": None,
                "internet_users_pct": round(internet_pct, 2),
                "n_obs_internet": len(net_vals),
            },
        }
