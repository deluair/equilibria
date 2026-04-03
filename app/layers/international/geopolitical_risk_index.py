"""Geopolitical Risk Index module.

Composite geopolitical risk from political stability and military burden. Countries
with low political stability (negative PV.EST) combined with high military spending
(as % GDP) face elevated geopolitical risk, reducing investment and trade (Caldara &
Iacoviello 2022; World Bank Worldwide Governance Indicators).

Score = 0.5 * instability_score + 0.5 * military_score, clipped to [0, 100].

Sources: WDI (PV.EST for political stability/absence of violence, MS.MIL.XPND.GD.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

# PV.EST ranges roughly from -2.5 to +2.5; below 0 = instability
# Military spending above this % of GDP is high burden
MILITARY_HIGH_THRESHOLD = 3.0  # percent of GDP


class GeopoliticalRiskIndex(LayerBase):
    layer_id = "lIN"
    name = "Geopolitical Risk Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        stability_rows = await db.fetch_all(
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

        military_rows = await db.fetch_all(
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

        if not stability_rows and not military_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no political stability or military spending data",
            }

        stability_values = [
            float(r["value"]) for r in stability_rows if r["value"] is not None
        ]
        military_values = [
            float(r["value"]) for r in military_rows if r["value"] is not None
        ]

        # Political instability score: PV.EST in [-2.5, +2.5], map to 0-100
        # PV.EST = -2.5 -> score 100 (maximum instability)
        # PV.EST = +2.5 -> score 0 (stable)
        if stability_values:
            avg_stability = float(np.mean(stability_values))
            instability_score = float(np.clip((-avg_stability + 2.5) / 5.0 * 100, 0, 100))
        else:
            avg_stability = None
            instability_score = 50.0  # unknown, neutral

        # Military burden score: above threshold -> stress
        if military_values:
            avg_military = float(np.mean(military_values))
            military_score = float(
                np.clip((avg_military / MILITARY_HIGH_THRESHOLD) * 50, 0, 100)
            )
        else:
            avg_military = None
            military_score = 0.0

        score = 0.5 * instability_score + 0.5 * military_score

        return {
            "score": round(score, 1),
            "country": country,
            "avg_political_stability": round(avg_stability, 4) if avg_stability is not None else None,
            "instability_score": round(instability_score, 1),
            "avg_military_pct_gdp": round(avg_military, 3) if avg_military is not None else None,
            "military_score": round(military_score, 1),
        }
