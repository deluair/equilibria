"""Trade Dispute Exposure module.

Estimates trade dispute risk by combining trade openness with political
stability/absence of violence. High trade integration in an unstable
political environment elevates the risk of trade conflicts and sanctions.

Score = clip(openness_component * 0.5 + instability_component * 0.5, 0, 100)

Sources: WDI
  NE.TRD.GNFS.ZS - Trade (% of GDP)
  PV.EST          - Political Stability and Absence of Violence: Estimate (WGI)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class TradeDisputeExposure(LayerBase):
    layer_id = "lTP"
    name = "Trade Dispute Exposure"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        openness_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.TRD.GNFS.ZS'
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        stability_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'PV.EST'
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not openness_rows and not stability_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no trade openness or stability data",
            }

        openness = None
        if openness_rows:
            vals = [float(r["value"]) for r in openness_rows if r["value"] is not None]
            if vals:
                openness = float(np.mean(vals[:5]))

        stability = None
        if stability_rows:
            vals = [float(r["value"]) for r in stability_rows if r["value"] is not None]
            if vals:
                stability = float(np.mean(vals[:3]))

        # Openness component: high trade exposure -> more at stake in disputes
        openness_component = 0.0
        if openness is not None:
            # Trade >100% GDP (small open economies) is very exposed
            openness_component = float(np.clip(openness * 0.6, 0, 60))

        # Instability component: PV.EST ranges roughly -2.5 to 2.5
        # Low (negative) stability = high dispute risk
        instability_component = 0.0
        if stability is not None:
            instability_component = float(np.clip((1.0 - stability) * 20, 0, 60))

        if openness is None and stability is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "all values null"}

        if openness is None:
            score = instability_component
        elif stability is None:
            score = openness_component
        else:
            score = openness_component * 0.5 + instability_component * 0.5

        score = float(np.clip(score, 0, 100))

        exposure_level = (
            "low" if score < 25
            else "moderate" if score < 50
            else "high" if score < 75
            else "critical"
        )

        return {
            "score": round(score, 1),
            "country": country,
            "trade_openness_pct_gdp": round(openness, 2) if openness is not None else None,
            "political_stability_estimate": round(stability, 4) if stability is not None else None,
            "openness_component": round(openness_component, 1),
            "instability_component": round(instability_component, 1),
            "dispute_exposure_level": exposure_level,
        }
