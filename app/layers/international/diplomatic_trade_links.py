"""Diplomatic Trade Links module.

Measures the stress on diplomatically-anchored trade relationships via trade openness
and export growth dynamics. Declining exports combined with already-low trade openness
signals deteriorating diplomatic-commercial relationships or market access barriers
(Rose 2004; Nitsch 2007).

Score = 0.5 * openness_stress + 0.5 * export_stress, clipped to [0, 100].

Sources: WDI (NE.TRD.GNFS.ZS trade openness, NE.EXP.GNFS.KD.ZG export growth)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

# Trade openness below this (% of GDP) signals low integration
OPENNESS_LOW_THRESHOLD = 30.0  # percent of GDP


class DiplomaticTradeLinks(LayerBase):
    layer_id = "lIN"
    name = "Diplomatic Trade Links"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        openness_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.TRD.GNFS.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        export_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.EXP.GNFS.KD.ZG'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not openness_rows and not export_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no trade openness or export growth data",
            }

        openness_values = [
            float(r["value"]) for r in openness_rows if r["value"] is not None
        ]
        export_values = [
            float(r["value"]) for r in export_rows if r["value"] is not None
        ]

        # Openness stress: low trade openness -> higher score
        if openness_values:
            avg_openness = float(np.mean(openness_values))
            openness_stress = float(
                np.clip((OPENNESS_LOW_THRESHOLD - avg_openness) * 2, 0, 100)
            )
        else:
            avg_openness = None
            openness_stress = 50.0

        # Export stress: negative or very low export growth -> higher score
        if export_values:
            avg_export_growth = float(np.mean(export_values))
            # Negative growth maps to stress, positive reduces stress
            export_stress = float(np.clip(-avg_export_growth * 4 + 20, 0, 100))
        else:
            avg_export_growth = None
            export_stress = 50.0

        score = 0.5 * openness_stress + 0.5 * export_stress

        return {
            "score": round(score, 1),
            "country": country,
            "avg_trade_openness_pct_gdp": round(avg_openness, 3) if avg_openness is not None else None,
            "avg_export_growth_pct": round(avg_export_growth, 3) if avg_export_growth is not None else None,
            "openness_stress": round(openness_stress, 1),
            "export_stress": round(export_stress, 1),
        }
