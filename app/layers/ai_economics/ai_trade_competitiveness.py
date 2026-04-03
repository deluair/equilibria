"""AI trade competitiveness: high-tech exports share as AI trade competitiveness proxy.

Countries leading in AI adoption gain trade competitiveness through productivity-
enhanced manufacturing, AI-enabled service exports (software, BPO, consulting),
and platform effects that create winner-take-all dynamics in digital trade. High-
technology exports as a share of manufactured exports (WDI TX.VAL.TECH.MF.ZS)
captures revealed comparative advantage in knowledge-intensive, AI-complementary
sectors.

WTO Digital Trade Estimates (2023): AI-enabled services trade growing at 2x the
rate of goods trade. Countries with established high-tech export bases are best
positioned to capture AI-driven trade gains.

Score: low high-tech export share -> CRISIS (excluded from AI trade gains),
high high-tech share -> STABLE (well-positioned for AI trade competitiveness).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class AITradeCompetitiveness(LayerBase):
    layer_id = "lAI"
    name = "AI Trade Competitiveness"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        hitech_code = "TX.VAL.TECH.MF.ZS"
        services_code = "BX.GSR.CMCP.ZS"

        hitech_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (hitech_code, "%high-technology exports%"),
        )
        services_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (services_code, "%computer%communications%"),
        )

        hitech_vals = [r["value"] for r in hitech_rows if r["value"] is not None]
        services_vals = [r["value"] for r in services_rows if r["value"] is not None]

        if not hitech_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for high-technology exports TX.VAL.TECH.MF.ZS",
            }

        hitech_share = hitech_vals[0]
        # Trend in high-tech exports
        trend = round(hitech_vals[0] - hitech_vals[-1], 3) if len(hitech_vals) > 1 else None
        services_share = services_vals[0] if services_vals else None

        # Score inversely mapped: higher high-tech exports = lower stress
        if hitech_share >= 30:
            base = 8.0
        elif hitech_share >= 20:
            base = 8.0 + (30.0 - hitech_share) * 1.5
        elif hitech_share >= 10:
            base = 23.0 + (20.0 - hitech_share) * 2.5
        elif hitech_share >= 5:
            base = 48.0 + (10.0 - hitech_share) * 3.0
        else:
            base = min(90.0, 63.0 + (5.0 - hitech_share) * 2.0)

        # Services trade (computer/communications) further reduces stress
        if services_share is not None:
            if services_share >= 15:
                base = max(5.0, base - 10.0)
            elif services_share >= 8:
                base = max(5.0, base - 5.0)

        # Improving trend slightly reduces stress
        if trend is not None and trend > 2.0:
            base = max(5.0, base - 5.0)
        elif trend is not None and trend < -2.0:
            base = min(100.0, base + 5.0)

        score = round(base, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "hitech_exports_pct_manufactured": round(hitech_share, 2),
                "hitech_exports_trend_change": trend,
                "ict_services_exports_pct": round(services_share, 2) if services_share is not None else None,
                "n_obs_hitech": len(hitech_vals),
                "n_obs_services": len(services_vals),
                "ai_trade_competitive": hitech_share >= 15,
                "trade_position": (
                    "leading" if hitech_share >= 25
                    else "competitive" if hitech_share >= 10
                    else "lagging"
                ),
            },
        }
