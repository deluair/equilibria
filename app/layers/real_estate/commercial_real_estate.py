"""Commercial Real Estate module.

Measures business environment quality for commercial activity. Uses ease of
doing business rank (IC.BUS.EASE.XQ) or business registration cost
(IC.REG.COST.PC.ZS) as fallback. High regulatory burden signals commercial
RE stress.

Score from ease of doing business: rank/190 * 100 (higher rank = more stress)
Score from registration cost: clip(cost * 2, 0, 100)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CommercialRealEstate(LayerBase):
    layer_id = "lRE"
    name = "Commercial Real Estate"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        ease_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IC.BUS.EASE.XQ'
            ORDER BY dp.date
            """,
            (country,),
        )

        reg_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IC.REG.COST.PC.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if ease_rows and len(ease_rows) >= 1:
            ease_vals = np.array([float(r["value"]) for r in ease_rows])
            latest_rank = float(ease_vals[-1])
            # Rank out of 190 economies; higher rank = worse business environment = more stress
            score = float(np.clip(latest_rank / 190.0 * 100.0, 0, 100))
            source = "IC.BUS.EASE.XQ"
            primary_value = round(latest_rank, 1)
            primary_label = "ease_of_doing_business_rank"
        elif reg_rows and len(reg_rows) >= 1:
            reg_vals = np.array([float(r["value"]) for r in reg_rows])
            latest_cost = float(reg_vals[-1])
            score = float(np.clip(latest_cost * 2.0, 0, 100))
            source = "IC.REG.COST.PC.ZS"
            primary_value = round(latest_cost, 2)
            primary_label = "business_registration_cost_pct_income"
        else:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient business environment data for commercial RE analysis",
            }

        return {
            "score": round(score, 1),
            "country": country,
            primary_label: primary_value,
            "data_source": source,
            "n_ease_obs": len(ease_rows) if ease_rows else 0,
            "n_reg_obs": len(reg_rows) if reg_rows else 0,
            "methodology": "ease of doing business rank / 190 * 100; fallback: reg cost * 2",
        }
