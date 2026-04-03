"""Wage coordination index: degree of coordination in wage-setting.

Wage coordination refers to the extent to which wage settlements in different
sectors and firms are coordinated — either through centralized bargaining,
pattern bargaining, or government intervention. The classic Calmfors-Driffill
(1988) hump-shape hypothesis suggests that both highly centralized and highly
decentralized systems outperform intermediate cases in wage moderation and
employment outcomes.

ICTWSS database codes:
    1 = fragmented firm/plant bargaining
    2 = mixed industry and firm bargaining
    3 = industry bargaining, limited coordination
    4 = centralized bargaining or government-imposed restraint
    5 = centralized bargaining with peace obligations

Stress scoring (higher coordination -> lower stress in this institutional context):
    score = clip((5 - coord_index) * 25, 0, 100)

    coord = 5 -> score = 0   (fully coordinated)
    coord = 3 -> score = 50
    coord = 1 -> score = 100 (fully fragmented)

Sources: ICTWSS / OECD (coord — wage coordination index, 1-5 scale)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

SERIES = "coord_wage"


class WageCoordinationIndex(LayerBase):
    layer_id = "lLI"
    name = "Wage Coordination Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'coord_wage'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no wage coordination data (coord_wage)",
            }

        latest_date = rows[0]["date"]
        coord = float(rows[0]["value"])

        score = float(np.clip((5.0 - coord) * 25.0, 0.0, 100.0))

        if coord >= 4.5:
            regime = "centralized"
        elif coord >= 3.5:
            regime = "coordinated"
        elif coord >= 2.5:
            regime = "mixed"
        elif coord >= 1.5:
            regime = "decentralized"
        else:
            regime = "fragmented"

        return {
            "score": round(score, 2),
            "country": country,
            "coord_index": round(coord, 2),
            "regime": regime,
            "latest_date": latest_date,
            "n_obs": len(rows),
            "note": (
                "score = clip((5 - coord) * 25, 0, 100). "
                "coord scale 1 (fragmented) to 5 (centralized). Series: coord_wage."
            ),
        }
