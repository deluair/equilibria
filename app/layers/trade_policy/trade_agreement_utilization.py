"""Trade Agreement Utilization module.

Proxies RTA utilization through export growth performance. Slow export
growth despite open trade stance signals poor trade agreement utilization
or preference erosion.

Score = clip(50 - export_growth * 5, 0, 100)

Sources: WDI
  NE.EXP.GNFS.KD.ZG - Exports of goods and services (annual % growth)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class TradeAgreementUtilization(LayerBase):
    layer_id = "lTP"
    name = "Trade Agreement Utilization"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.EXP.GNFS.KD.ZG'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient export growth data"}

        valid = [float(r["value"]) for r in rows if r["value"] is not None]
        if len(valid) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient valid observations"}

        dates = [r["date"] for r in rows if r["value"] is not None]
        values = np.array(valid)

        mean_growth = float(np.mean(values))
        recent_growth = float(np.mean(values[-5:]))

        # Trend in export growth
        t = np.arange(len(values), dtype=float)
        slope, _, r_value, p_value, _ = linregress(t, values)

        # Low or declining export growth = poor RTA utilization
        score = float(np.clip(50 - recent_growth * 5, 0, 100))

        utilization = (
            "high" if recent_growth > 5
            else "moderate" if recent_growth > 1
            else "low" if recent_growth > -2
            else "very low / declining"
        )

        return {
            "score": round(score, 1),
            "country": country,
            "mean_export_growth_pct": round(mean_growth, 2),
            "recent_export_growth_pct": round(recent_growth, 2),
            "growth_trend_slope": round(float(slope), 4),
            "r_squared": round(float(r_value**2), 4),
            "p_value": round(float(p_value), 4),
            "rta_utilization_proxy": utilization,
            "period": f"{dates[0]} to {dates[-1]}",
            "n_obs": len(values),
        }
