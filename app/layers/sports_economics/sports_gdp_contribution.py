"""Sports GDP contribution: sports industry value added as share of GDP.

Recreation and entertainment services (WDI proxy) capture the economic weight
of sports-related activity within national output. A rising share signals a
maturing sports economy; suppressed values indicate underdeveloped commercial
sports infrastructure or data gaps in informal sector activity.

Score: very low share (<0.5%) -> STABLE emerging, moderate (0.5-1.5%) ->
WATCH developing sector, high (1.5-3%) -> STRESS saturating, very high (>3%)
-> CRISIS over-reliance on discretionary spend with cyclical vulnerability.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class SportsGDPContribution(LayerBase):
    layer_id = "lSP"
    name = "Sports GDP Contribution"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "IS.SRV.MISC.ZS"
        name = "recreation"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        values = [r["value"] for r in rows if r["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for IS.SRV.MISC.ZS"}

        latest = values[0]
        trend = round(values[0] - values[-1], 3) if len(values) > 1 else None

        if latest < 0.5:
            score = 8.0 + latest * 30.0
        elif latest < 1.5:
            score = 23.0 + (latest - 0.5) * 22.0
        elif latest < 3.0:
            score = 45.0 + (latest - 1.5) * 20.0
        else:
            score = min(100.0, 75.0 + (latest - 3.0) * 8.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "recreation_services_gdp_share_pct": round(latest, 3),
                "trend_pct_change": trend,
                "n_obs": len(values),
                "sector_stage": (
                    "emerging" if latest < 0.5
                    else "developing" if latest < 1.5
                    else "saturating" if latest < 3.0
                    else "dominant"
                ),
            },
        }
