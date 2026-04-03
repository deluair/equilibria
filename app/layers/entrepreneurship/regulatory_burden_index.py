"""Regulatory Burden Index module.

Measures the cost and time required to start a business (Doing Business indicators).
Uses World Bank WDI:
- IC.REG.DURS: Time required to start a business (days)
- IC.REG.COST.PC.ZS: Cost of business start-up procedures (% of GNI per capita)

Long setup times and high costs deter entrepreneurship, particularly for
informal sector participants and low-income entrepreneurs. Lower burden
correlates with higher business formation and formalization rates.

Score: higher score = heavier regulatory burden = more stress.

Sources: World Bank WDI (Doing Business)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class RegulatoryBurdenIndex(LayerBase):
    layer_id = "lER"
    name = "Regulatory Burden Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        days_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IC.REG.DURS'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        cost_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IC.REG.COST.PC.ZS'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        days: float | None = None
        cost_pct: float | None = None

        if days_rows:
            vals = [float(r["value"]) for r in days_rows if r["value"] is not None]
            days = float(np.mean(vals)) if vals else None

        if cost_rows:
            vals = [float(r["value"]) for r in cost_rows if r["value"] is not None]
            cost_pct = float(np.mean(vals)) if vals else None

        if days is None and cost_pct is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no Doing Business data available"}

        score_parts: list[float] = []

        if days is not None:
            # Days to start: 0-100 days range. Higher = more burden.
            days_score = min(100.0, (days / 60.0) * 100.0)
            score_parts.append(days_score)

        if cost_pct is not None:
            # Cost as % GNI per capita: 0-100%+. Higher = more burden.
            cost_score = min(100.0, (cost_pct / 50.0) * 100.0)
            score_parts.append(cost_score)

        score = float(np.mean(score_parts))

        return {
            "score": round(score, 1),
            "country": country,
            "days_to_start_business": round(days, 1) if days is not None else None,
            "startup_cost_pct_gni": round(cost_pct, 2) if cost_pct is not None else None,
            "interpretation": "High score = high days/cost to start business = heavy regulatory burden",
        }
