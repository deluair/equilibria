"""Arts employment share: arts, culture, and recreation employment fraction.

Creative and cultural employment spans performing arts, visual arts, design,
museums, libraries, and recreation. Proxied by total services employment share
(SL.SRV.EMPL.ZS), which encompasses the broad tertiary sector where cultural
occupations concentrate. A higher share indicates a more post-industrial
economy with capacity for arts-sector job creation.

Score: very low services employment -> STABLE pre-transition, moderate -> WATCH
growing services economy, high -> STRESS mature services-dominant, very high
-> CRISIS potential over-servicification or hollowing-out of productive sectors.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class ArtsEmploymentShare(LayerBase):
    layer_id = "lAR"
    name = "Arts Employment Share"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "SL.SRV.EMPL.ZS"
        name = "Employment in services"

        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for SL.SRV.EMPL.ZS",
            }

        vals = [r["value"] for r in rows if r["value"] is not None]
        if not vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "all null values"}

        latest = vals[0]
        trend = round(vals[0] - vals[-1], 3) if len(vals) > 1 else None

        # Arts/culture employment is roughly 2-4% of total employment in developed economies,
        # concentrated within the services sector. Score reflects services sector maturity.
        # <30% = STABLE (agrarian/industrial), 30-55% = WATCH, 55-70% = STRESS, >70% = CRISIS
        if latest < 30.0:
            score = 5.0 + latest * 0.5
        elif latest < 55.0:
            score = 20.0 + (latest - 30.0) * 1.2
        elif latest < 70.0:
            score = 50.0 + (latest - 55.0) * 1.67
        else:
            score = min(100.0, 75.05 + (latest - 70.0) * 0.83)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "services_employment_share_pct": round(latest, 2),
                "trend_pct_change": trend,
                "n_obs": len(vals),
                "services_stage": (
                    "agrarian" if latest < 30.0
                    else "transitioning" if latest < 55.0
                    else "services-dominant" if latest < 70.0
                    else "post-industrial"
                ),
            },
        }
