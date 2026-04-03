"""Digital Infrastructure Score module.

Measures broadband penetration and digital connectivity as a share of population.
Low penetration signals inadequate digital infrastructure.

Sources: WDI IT.NET.BBND.P2 (fixed broadband subscriptions per 100 people),
         WDI IT.NET.USER.ZS (individuals using the internet, % of population).
Score = clip(100 - internet_use_pct, 0, 100).
High internet use -> low stress score.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class DigitalInfrastructureScore(LayerBase):
    layer_id = "lIF"
    name = "Digital Infrastructure Score"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        bb_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IT.NET.BBND.P2'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        inet_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IT.NET.USER.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not bb_rows and not inet_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        bb_per100 = float(bb_rows[0]["value"]) if bb_rows else None
        inet_pct = float(inet_rows[0]["value"]) if inet_rows else None

        if inet_pct is not None:
            gap = float(np.clip(100.0 - inet_pct, 0, 100))
        else:
            # Broadband per 100 capped at 100 as proxy
            gap = float(np.clip(100.0 - min(bb_per100, 100.0), 0, 100))

        return {
            "score": round(gap, 1),
            "country": country,
            "broadband_per_100": round(bb_per100, 2) if bb_per100 is not None else None,
            "internet_users_pct": round(inet_pct, 2) if inet_pct is not None else None,
            "digital_gap_ppt": round(gap, 2),
            "interpretation": (
                "Very low digital connectivity: major barrier to digital economy"
                if gap > 60
                else "Significant digital divide" if gap > 40
                else "Moderate digital gap" if gap > 20
                else "Broad digital connectivity"
            ),
            "_sources": ["WDI:IT.NET.BBND.P2", "WDI:IT.NET.USER.ZS"],
        }
