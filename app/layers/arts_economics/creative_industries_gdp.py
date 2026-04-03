"""Creative industries GDP: creative sector value added as share of GDP.

Creative industries encompass cultural services, digital content, design, and
media. Proxied by cultural and commercial services exports (BX.GSR.CMCP.ZS)
combined with ICT services exports share (BX.GSR.CCIS.ZS) as share of GDP.
Higher creative sector participation signals a more diversified, knowledge-
intensive economy.

Score: low creative share -> STABLE traditional economy, rising share -> WATCH
transition, high share -> STRESS structural dependency on creative output,
very high -> CRISIS over-concentration in soft sectors.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class CreativeIndustriesGDP(LayerBase):
    layer_id = "lAR"
    name = "Creative Industries GDP"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        cult_code = "BX.GSR.CMCP.ZS"
        ict_code = "BX.GSR.CCIS.ZS"

        cult_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (cult_code, "%cultural%services%"),
        )
        ict_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (ict_code, "%ICT service%"),
        )

        cult_vals = [r["value"] for r in cult_rows if r["value"] is not None]
        ict_vals = [r["value"] for r in ict_rows if r["value"] is not None]

        if not cult_vals and not ict_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for BX.GSR.CMCP.ZS or BX.GSR.CCIS.ZS",
            }

        cult_latest = cult_vals[0] if cult_vals else 0.0
        ict_latest = ict_vals[0] if ict_vals else 0.0

        # Combined proxy: cultural services + ICT services as % of total services exports
        creative_share = cult_latest + ict_latest
        trend = None
        if len(cult_vals) > 1:
            trend = round(cult_vals[0] - cult_vals[-1], 3)

        # Score: creative share as % of services exports
        # <5% = STABLE (low creative intensity), 5-15% = WATCH, 15-30% = STRESS, >30% = CRISIS
        if creative_share < 5.0:
            score = 5.0 + creative_share * 2.0
        elif creative_share < 15.0:
            score = 15.0 + (creative_share - 5.0) * 2.5
        elif creative_share < 30.0:
            score = 40.0 + (creative_share - 15.0) * 2.0
        else:
            score = min(100.0, 70.0 + (creative_share - 30.0) * 1.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "cultural_services_exports_pct": round(cult_latest, 3),
                "ict_services_exports_pct": round(ict_latest, 3),
                "creative_share_combined": round(creative_share, 3),
                "trend_cultural_pct": trend,
                "n_obs_cultural": len(cult_vals),
                "n_obs_ict": len(ict_vals),
            },
        }
