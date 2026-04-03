"""Entry Barriers module.

Measures barriers to market entry using World Bank Doing Business indicators:
- Business registration cost as a percentage of GNI per capita (IC.REG.COST.PC.ZS)
- Ease of Doing Business score or rank (IC.BUS.EASE.XQ)

High registration costs combined with a poor ease-of-doing-business environment
signal elevated barriers that protect incumbents from new competition.

Scoring:
  score = clip(cost_penalty + rank_penalty, 0, 100)
  cost_penalty = clip(cost_pct_gni * 5, 0, 50)
  rank_penalty = clip(rank / 4, 0, 50)   (rank 0=best, 200=worst)

Sources: WDI (IC.REG.COST.PC.ZS, IC.BUS.EASE.XQ)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class EntryBarriers(LayerBase):
    layer_id = "lCO"
    name = "Entry Barriers"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        cost_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IC.REG.COST.PC.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        ease_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IC.BUS.EASE.XQ'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not cost_rows and not ease_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no entry barrier data"}

        def latest_value(rows) -> float | None:
            for r in rows:
                if r["value"] is not None:
                    try:
                        return float(r["value"])
                    except (TypeError, ValueError):
                        pass
            return None

        cost = latest_value(cost_rows)
        ease = latest_value(ease_rows)

        cost_penalty = float(np.clip((cost * 5) if cost is not None else 0, 0, 50))
        rank_penalty = float(np.clip((ease / 4) if ease is not None else 0, 0, 50))

        score = float(np.clip(cost_penalty + rank_penalty, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "registration_cost_pct_gni": round(cost, 2) if cost is not None else None,
            "ease_of_business_rank": round(ease, 1) if ease is not None else None,
            "cost_penalty": round(cost_penalty, 2),
            "rank_penalty": round(rank_penalty, 2),
            "interpretation": (
                "low barriers" if score < 33
                else "moderate barriers" if score < 66
                else "high entry barriers"
            ),
            "reference": "World Bank Doing Business (2020 vintage); Djankov et al. (2002)",
        }
