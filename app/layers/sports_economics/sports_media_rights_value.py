"""Sports media rights value: broadcast rights revenue growth proxy.

ICT services exports (WDI TX.SRV.TELE.ZS) and information/communication
services exports (BX.GSR.CMCP.ZS) jointly proxy the digital and broadcast
distribution infrastructure through which sports rights are monetized.
Rapid growth in ICT services exports tracks the structural shift toward
digital streaming rights revenue, now the dominant valuation driver for
professional leagues and federations globally.

Score: low ICT export share (<2%) -> STABLE small market; moderate (2-5%)
-> WATCH growing ecosystem; high (5-10%) -> STRESS bid inflation risk;
very high (>10%) -> CRISIS rights bubble territory.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class SportMediaRightsValue(LayerBase):
    layer_id = "lSP"
    name = "Sports Media Rights Value"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "BX.GSR.CMCP.ZS"
        name = "communications"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        values = [r["value"] for r in rows if r["value"] is not None]
        if not values:
            # Fallback to ICT services
            ict_code = "TX.SRV.TELE.ZS"
            rows = await db.fetch_all(
                "SELECT value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
                "ORDER BY date DESC LIMIT 15",
                (ict_code, "%ICT%"),
            )
            values = [r["value"] for r in rows if r["value"] is not None]

        if not values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for BX.GSR.CMCP.ZS or TX.SRV.TELE.ZS",
            }

        latest = values[0]
        trend = round(values[0] - values[-1], 3) if len(values) > 1 else None

        if latest < 2.0:
            score = 8.0 + latest * 8.5
        elif latest < 5.0:
            score = 25.0 + (latest - 2.0) * 8.3
        elif latest < 10.0:
            score = 50.0 + (latest - 5.0) * 5.0
        else:
            score = min(100.0, 75.0 + (latest - 10.0) * 2.5)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "ict_comms_export_share_pct": round(latest, 3),
                "trend_pct_change": trend,
                "n_obs": len(values),
                "rights_market_stage": (
                    "nascent" if latest < 2.0
                    else "growing" if latest < 5.0
                    else "mature" if latest < 10.0
                    else "inflated"
                ),
            },
        }
