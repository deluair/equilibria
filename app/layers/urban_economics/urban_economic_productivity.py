"""Urban Economic Productivity module.

Measures whether urbanization is translating into agglomeration gains
via services sector growth. Rising services share alongside urbanization
indicates productive agglomeration. Stagnant services despite urbanization
signals a productivity gap (urbanization without growth).

Sources: WDI NV.SRV.TOTL.ZS (services value added, % of GDP),
         WDI SP.URB.GROW (urban population growth rate).
Uses trend in services share over available data.
Score: if services rising with urbanization -> low stress; if urbanization high
but services stagnant or falling -> high stress.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class UrbanEconomicProductivity(LayerBase):
    layer_id = "lUE"
    name = "Urban Economic Productivity"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        srv_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NV.SRV.TOTL.ZS'
            ORDER BY dp.date ASC
            LIMIT 20
            """,
            (country,),
        )

        urb_growth_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SP.URB.GROW'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not srv_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no services data"}

        urb_growth = float(urb_growth_rows[0]["value"]) if urb_growth_rows else None

        srv_values = np.array([float(r["value"]) for r in srv_rows])
        srv_latest = float(srv_values[-1])

        # Estimate services trend via linear regression slope
        n = len(srv_values)
        if n >= 3:
            x = np.arange(n, dtype=float)
            slope = float(np.polyfit(x, srv_values, 1)[0])
        else:
            slope = 0.0

        # Productivity gap logic:
        # Urbanization high + services growing (slope > 0) -> low stress
        # Urbanization high + services stagnant/falling -> high stress
        if urb_growth is not None and urb_growth > 0:
            # Normalize: >2%/yr urbanization with no services growth = stress
            productivity_gap = max(0.0, urb_growth - max(0.0, slope * 5))
            score = float(np.clip(productivity_gap * 15, 0, 100))
        else:
            # No urbanization growth context: use services level as proxy
            # Low services share (<40%) despite some urbanization = stress
            score = float(np.clip(max(0.0, 40.0 - srv_latest) * 2.0, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "services_value_added_pct_gdp": round(srv_latest, 2),
            "services_trend_slope_ppt_per_yr": round(slope, 4),
            "urban_growth_rate_pct": round(urb_growth, 3) if urb_growth is not None else None,
            "n_observations": n,
            "interpretation": (
                "Urbanization without agglomeration gains: services stagnant relative to urbanization rate"
                if score > 50
                else "Partial productivity gap: urbanization outpacing services growth" if score > 25
                else "Urbanization tracking services growth: agglomeration gains evident" if score < 15
                else "Moderate productivity-urbanization alignment"
            ),
            "_sources": ["WDI:NV.SRV.TOTL.ZS", "WDI:SP.URB.GROW"],
        }
