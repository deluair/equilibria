"""Music and film industry economics: entertainment sector economic contribution.

The music and film industries are the highest-revenue segments of the creative
economy, with global recorded music exceeding $28B and film/TV production
exceeding $100B annually. Proxied by ICT services exports as % of total services
(BX.GSR.CCIS.ZS) as the dominant digital distribution channel, and fixed
broadband subscriptions per 100 people (IT.NET.BBND.P2) as infrastructure
enabling streaming -- the primary revenue model for both industries.

Score: low broadband + low ICT exports -> STABLE (limited entertainment
market), moderate -> WATCH emerging consumption base, high -> STRESS active
digital entertainment economy, very high -> CRISIS potential market saturation
or platform dependency.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class MusicFilmIndustryEconomics(LayerBase):
    layer_id = "lAR"
    name = "Music and Film Industry Economics"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        ict_code = "BX.GSR.CCIS.ZS"
        bb_code = "IT.NET.BBND.P2"

        ict_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (ict_code, "%ICT service%"),
        )
        bb_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (bb_code, "%Fixed broadband%"),
        )

        ict_vals = [r["value"] for r in ict_rows if r["value"] is not None]
        bb_vals = [r["value"] for r in bb_rows if r["value"] is not None]

        if not ict_vals and not bb_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for BX.GSR.CCIS.ZS or IT.NET.BBND.P2",
            }

        ict_latest = ict_vals[0] if ict_vals else 0.0
        bb_latest = bb_vals[0] if bb_vals else 0.0

        bb_trend = round(bb_vals[0] - bb_vals[-1], 3) if len(bb_vals) > 1 else None
        ict_trend = round(ict_vals[0] - ict_vals[-1], 3) if len(ict_vals) > 1 else None

        # Broadband: per 100 people; global range 0-50+; >40 = highly connected
        bb_norm = min(100.0, bb_latest / 45.0 * 100.0)

        # ICT services exports: 0-50%+ of services; normalize to 0-100
        ict_norm = min(100.0, ict_latest / 50.0 * 100.0)

        # Combined: 50% broadband (consumption infrastructure), 50% ICT exports (production)
        score = 0.50 * bb_norm + 0.50 * ict_norm

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "ict_services_exports_pct": round(ict_latest, 3),
                "fixed_broadband_per100": round(bb_latest, 2),
                "broadband_norm_score": round(bb_norm, 2),
                "ict_norm_score": round(ict_norm, 2),
                "trend_broadband": bb_trend,
                "trend_ict_exports": ict_trend,
                "n_obs_ict": len(ict_vals),
                "n_obs_broadband": len(bb_vals),
            },
        }
