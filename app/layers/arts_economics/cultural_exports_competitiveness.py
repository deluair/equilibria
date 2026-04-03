"""Cultural exports competitiveness: cultural goods and services export strength.

Cultural exports include audiovisual services, printed media, performing arts,
design goods, and heritage crafts. Proxied by cultural and commercial services
exports (BX.GSR.CMCP.ZS) and high-tech manufactured exports (TX.VAL.TECH.MF.ZS)
as a share of manufactured exports -- the latter capturing design-embedded goods.

Score: low cultural export participation -> STABLE domestic-oriented culture
economy, moderate -> WATCH emerging export capacity, high -> STRESS active
cultural exporter, very high -> CRISIS potential over-reliance on soft-power
exports or digital platform dominance.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class CulturalExportsCompetitiveness(LayerBase):
    layer_id = "lAR"
    name = "Cultural Exports Competitiveness"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        cult_code = "BX.GSR.CMCP.ZS"
        tech_code = "TX.VAL.TECH.MF.ZS"

        cult_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (cult_code, "%cultural%"),
        )
        tech_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (tech_code, "%high-technology exports%"),
        )

        cult_vals = [r["value"] for r in cult_rows if r["value"] is not None]
        tech_vals = [r["value"] for r in tech_rows if r["value"] is not None]

        if not cult_vals and not tech_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for BX.GSR.CMCP.ZS or TX.VAL.TECH.MF.ZS",
            }

        cult_latest = cult_vals[0] if cult_vals else 0.0
        tech_latest = tech_vals[0] if tech_vals else 0.0

        cult_trend = round(cult_vals[0] - cult_vals[-1], 3) if len(cult_vals) > 1 else None
        tech_trend = round(tech_vals[0] - tech_vals[-1], 3) if len(tech_vals) > 1 else None

        # Normalize each: cultural services 0-30% range, high-tech 0-60% range
        cult_norm = min(100.0, cult_latest / 30.0 * 100.0)
        tech_norm = min(100.0, tech_latest / 60.0 * 100.0)

        # Composite: 60% weight on cultural services (more direct), 40% on high-tech
        score = 0.60 * cult_norm + 0.40 * tech_norm

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "cultural_services_exports_pct": round(cult_latest, 3),
                "hightech_exports_mfg_pct": round(tech_latest, 3),
                "cultural_norm_score": round(cult_norm, 2),
                "tech_norm_score": round(tech_norm, 2),
                "trend_cultural": cult_trend,
                "trend_hightech": tech_trend,
                "n_obs_cultural": len(cult_vals),
                "n_obs_tech": len(tech_vals),
            },
        }
