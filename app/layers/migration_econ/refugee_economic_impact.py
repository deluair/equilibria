"""Refugee Economic Impact module.

Proxies refugee and displacement pressure using political instability
and military expenditure as indicators of conflict-driven displacement.

Severe political instability combined with high military spending
signals active conflict or near-conflict conditions that generate
both refugee outflows and economic disruption.

Score reflects composite displacement risk from instability and
militarization, which correlate with mass population movements.

Sources: WDI (PV.EST, MS.MIL.XPND.GD.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class RefugeeEconomicImpact(LayerBase):
    layer_id = "lME"
    name = "Refugee Economic Impact"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        stab_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'PV.EST'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        mil_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'MS.MIL.XPND.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not stab_rows and not mil_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        stab_vals = [float(r["value"]) for r in stab_rows if r["value"] is not None]
        mil_vals = [float(r["value"]) for r in mil_rows if r["value"] is not None]

        stab = float(np.mean(stab_vals)) if stab_vals else 0.0
        mil = float(np.mean(mil_vals)) if mil_vals else 2.0

        # Instability component: PV.EST -2.5 to +2.5; lower = worse
        instab_raw = max(0.0, -stab)
        instab_score = float(np.clip(instab_raw * 24, 0, 60))

        # Military expenditure: high % GDP = conflict signal (>4% = extreme)
        mil_score = float(np.clip(mil / 4 * 40, 0, 40))

        score = instab_score + mil_score

        return {
            "score": round(score, 1),
            "country": country,
            "political_stability_est": round(stab, 4),
            "military_expenditure_pct_gdp": round(mil, 2),
            "components": {
                "instability_score": round(instab_score, 2),
                "militarization_score": round(mil_score, 2),
            },
            "interpretation": (
                "high displacement risk" if score > 65
                else "moderate displacement risk" if score > 40
                else "low displacement risk"
            ),
        }
