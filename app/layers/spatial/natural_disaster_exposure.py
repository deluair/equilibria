"""Natural disaster exposure: climate hazard and extreme weather vulnerability.

Countries exposed to frequent droughts, floods, and extreme temperature events
face spatial vulnerability that impairs agricultural productivity, displaces
populations, and disrupts economic activity. High agricultural land share
in climate-vulnerable regions compounds this risk.

Primary indicator: EN.CLC.MDAT.ZS (population affected by droughts, floods,
extreme temps as % of total population). If unavailable, fall back to
AG.LND.AGRI.ZS (agricultural land %) as land vulnerability proxy.

Score:
    Primary: score = clip(disaster_exposure_pct * 2, 0, 100)
    Fallback: score = clip((agri_land_pct - 30) * 1.5, 0, 100)

References:
    UNDRR (2020). Global Assessment Report on Disaster Risk Reduction.
    Noy, I. (2009). The Macroeconomic Consequences of Disasters. Journal of
        Development Economics, 88(2), 221-231.
    Hsiang, S. & Jina, A. (2014). The Causal Effect of Environmental Catastrophe
        on Long-Run Economic Growth. NBER WP 20352.

Sources: World Bank WDI EN.CLC.MDAT.ZS, AG.LND.AGRI.ZS.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class NaturalDisasterExposure(LayerBase):
    layer_id = "l11"
    name = "Natural Disaster Exposure"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        # Primary: disaster-affected population share
        disaster_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'EN.CLC.MDAT.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        # Fallback: agricultural land share
        agri_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'AG.LND.AGRI.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        disaster_pct = None
        disaster_year = None
        agri_pct = None
        used_fallback = False

        if disaster_rows:
            disaster_pct = float(disaster_rows[0]["value"])
            disaster_year = disaster_rows[0]["date"]
            score = float(np.clip(disaster_pct * 2.0, 0.0, 100.0))
        elif agri_rows:
            agri_pct = float(agri_rows[0]["value"])
            score = float(np.clip((agri_pct - 30.0) * 1.5, 0.0, 100.0))
            used_fallback = True
        else:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no disaster exposure or agricultural land data",
                "country": country,
            }

        # Average over available disaster observations for trend
        trend_slope = None
        if disaster_rows and len(disaster_rows) >= 3:
            vals = np.array([float(r["value"]) for r in reversed(disaster_rows)])
            t = np.arange(len(vals), dtype=float)
            trend_slope = round(float(np.polyfit(t, vals, 1)[0]), 4)

        return {
            "score": round(score, 2),
            "country": country,
            "disaster_affected_pct": round(disaster_pct, 3) if disaster_pct is not None else None,
            "disaster_year": disaster_year,
            "agri_land_pct": round(agri_pct, 2) if agri_pct is not None else None,
            "used_fallback_indicator": used_fallback,
            "trend_slope_pp_per_yr": trend_slope,
            "exposure_level": (
                "extreme" if score > 75
                else "high" if score > 50
                else "moderate" if score > 25
                else "low"
            ),
            "n_obs": len(disaster_rows) if disaster_rows else len(agri_rows),
            "_source": (
                "WDI EN.CLC.MDAT.ZS" if not used_fallback
                else "WDI AG.LND.AGRI.ZS (fallback)"
            ),
        }
