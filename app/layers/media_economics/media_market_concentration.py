"""Media market concentration: internet and telecom penetration as market structure proxy.

High mobile and internet penetration accompanied by concentrated telecom revenue
signals oligopolistic media markets where a few platforms or carriers control
the information infrastructure. This suppresses media pluralism, raises entry
barriers for independent journalism, and concentrates economic rents.

Score: high internet penetration with competitive spread -> STABLE; very high
penetration with concentrated revenue -> WATCH or STRESS; low penetration with
dominant incumbents -> CRISIS for media market health.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class MediaMarketConcentration(LayerBase):
    layer_id = "lMD"
    name = "Media Market Concentration"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        net_code = "IT.NET.USER.ZS"
        mob_code = "IT.CEL.SETS.P2"

        net_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (net_code, "%internet users%"),
        )
        mob_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (mob_code, "%mobile cellular%"),
        )

        net_vals = [r["value"] for r in net_rows if r["value"] is not None]
        mob_vals = [r["value"] for r in mob_rows if r["value"] is not None]

        if not net_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for internet users IT.NET.USER.ZS",
            }

        net_pct = net_vals[0]
        mob_per100 = mob_vals[0] if mob_vals else None

        # Mobile subscriptions per 100 people above 100 signals saturation
        # and potential oligopolistic lock-in via bundled media services
        mob_ratio = (mob_per100 / 100.0) if mob_per100 is not None else 1.0
        mob_ratio = min(mob_ratio, 2.0)  # cap at 200 subs/100 (dual-SIM saturation)

        # Base score: low internet penetration -> higher market concentration risk
        if net_pct >= 80:
            base = 20.0
        elif net_pct >= 60:
            base = 30.0 + (80 - net_pct) * 0.5
        elif net_pct >= 40:
            base = 40.0 + (60 - net_pct) * 0.75
        elif net_pct >= 20:
            base = 55.0 + (40 - net_pct) * 0.75
        else:
            base = min(100.0, 70.0 + (20 - net_pct) * 1.0)

        # High mobile saturation with low internet access -> carrier concentration risk
        if mob_ratio >= 1.5 and net_pct < 50:
            base = min(100.0, base + 10.0)
        elif mob_ratio >= 1.0 and net_pct >= 70:
            base = max(10.0, base - 5.0)  # competitive open market signal

        score = round(base, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "internet_users_pct": round(net_pct, 2),
                "mobile_per_100": round(mob_per100, 2) if mob_per100 is not None else None,
                "mobile_saturation_ratio": round(mob_ratio, 3),
                "n_obs_internet": len(net_vals),
                "n_obs_mobile": len(mob_vals),
                "concentration_risk": net_pct < 50 and mob_ratio >= 1.2,
            },
        }
