"""Esports and digital economy: digital entertainment sector growth as esports proxy.

ICT goods and services exports (WDI TX.VAL.ICTG.ZS.UN or BX.GSR.CMCP.ZS)
measure the scale of a country's digital production and export capacity.
Esports revenue -- streaming, tournament prize pools, sponsorship, in-game
purchases -- is structurally embedded within the broader digital entertainment
and ICT services ecosystem. High ICT export intensity combined with rapid
growth is the most reliable publicly available proxy for esports market depth.

Score: ICT export share of total exports. <1% -> STABLE pre-esports economy;
1-5% -> WATCH digital infrastructure forming; 5-15% -> STRESS monetization
pressure from youth entertainment substitution; >15% -> CRISIS structural
displacement of traditional sports consumption.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class EsportsDigitalEconomy(LayerBase):
    layer_id = "lSP"
    name = "Esports Digital Economy"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "TX.VAL.ICTG.ZS.UN"
        name = "ICT goods"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        values = [r["value"] for r in rows if r["value"] is not None]

        if not values:
            # Fallback: communications services exports
            fallback_code = "BX.GSR.CMCP.ZS"
            rows = await db.fetch_all(
                "SELECT value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
                "ORDER BY date DESC LIMIT 15",
                (fallback_code, "%communications%"),
            )
            values = [r["value"] for r in rows if r["value"] is not None]

        if not values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for TX.VAL.ICTG.ZS.UN or BX.GSR.CMCP.ZS",
            }

        latest = values[0]
        trend = round(values[0] - values[-1], 3) if len(values) > 1 else None

        if latest < 1.0:
            score = 5.0 + latest * 15.0
        elif latest < 5.0:
            score = 20.0 + (latest - 1.0) * 7.5
        elif latest < 15.0:
            score = 50.0 + (latest - 5.0) * 2.5
        else:
            score = min(100.0, 75.0 + (latest - 15.0) * 1.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "ict_export_share_pct": round(latest, 3),
                "trend_pct_change": trend,
                "n_obs": len(values),
                "digital_maturity": (
                    "pre-digital" if latest < 1.0
                    else "emerging" if latest < 5.0
                    else "established" if latest < 15.0
                    else "dominant"
                ),
            },
        }
