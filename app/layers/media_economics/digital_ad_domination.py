"""Digital ad domination: ICT sector revenue concentration and platform capture.

Digital advertising revenue has become the oxygen of the media economy. When
ICT sector revenue is highly concentrated and GDP-relative ICT value-added is
large, it signals that platform economics have captured advertising markets
previously distributed across local and national media outlets. This hollows
out the economic base for independent journalism.

Score: diversified ICT contribution with moderate growth -> STABLE; high ICT
share suggesting platform-led consolidation -> WATCH; rapid ICT expansion
displacing legacy media economics -> STRESS; extreme concentration -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class DigitalAdDomination(LayerBase):
    layer_id = "lMD"
    name = "Digital Ad Domination"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        ict_code = "NV.SRV.TETC.ZS"
        net_code = "IT.NET.USER.ZS"

        ict_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (ict_code, "%ICT service%"),
        )
        net_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (net_code, "%internet users%"),
        )

        ict_vals = [r["value"] for r in ict_rows if r["value"] is not None]
        net_vals = [r["value"] for r in net_rows if r["value"] is not None]

        if not net_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for internet users IT.NET.USER.ZS",
            }

        net_pct = net_vals[0]
        ict_pct = ict_vals[0] if ict_vals else None

        # Without ICT data, approximate from internet penetration alone
        if ict_pct is None:
            # High internet penetration without measured ICT -> unknown platform capture
            if net_pct >= 80:
                base = 55.0
            elif net_pct >= 50:
                base = 40.0 + (net_pct - 50) * 0.5
            else:
                base = 30.0 + net_pct * 0.2
            return {
                "score": round(base, 2),
                "signal": self.classify_signal(round(base, 2)),
                "metrics": {
                    "internet_users_pct": round(net_pct, 2),
                    "ict_services_gdp_pct": None,
                    "n_obs_ict": 0,
                    "n_obs_internet": len(net_vals),
                    "proxy_only": True,
                },
            }

        # ICT services as % of GDP * internet penetration = platform economy depth
        platform_depth = ict_pct * (net_pct / 100.0)

        # Score: higher platform depth -> higher ad domination risk for legacy media
        if platform_depth < 1.0:
            base = 15.0 + platform_depth * 10.0
        elif platform_depth < 3.0:
            base = 25.0 + (platform_depth - 1.0) * 12.5
        elif platform_depth < 6.0:
            base = 50.0 + (platform_depth - 3.0) * 8.0
        else:
            base = min(100.0, 74.0 + (platform_depth - 6.0) * 3.0)

        score = round(base, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "internet_users_pct": round(net_pct, 2),
                "ict_services_gdp_pct": round(ict_pct, 2),
                "platform_depth_index": round(platform_depth, 4),
                "n_obs_ict": len(ict_vals),
                "n_obs_internet": len(net_vals),
                "proxy_only": False,
            },
        }
