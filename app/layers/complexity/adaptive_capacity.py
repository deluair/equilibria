"""Adaptive Capacity module.

System adaptability measured by structural change speed and governance quality.
Low structural change + poor governance = low adaptive capacity = high stress.

Composite: 0.5 * (100 - change_score) + 0.5 * governance_stress

Sources: WDI NV.AGR.TOTL.ZS, NV.IND.TOTL.ZS for change speed; WDI GE.EST (government effectiveness)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class AdaptiveCapacity(LayerBase):
    layer_id = "lCP"
    name = "Adaptive Capacity"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        # --- Structural change speed ---
        change_scores: list[float] = []
        sector_dates: dict[str, str] = {}

        for series_id, label in [
            ("NV.AGR.TOTL.ZS", "agriculture"),
            ("NV.IND.TOTL.ZS", "industry"),
        ]:
            rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                ORDER BY dp.date
                """,
                (country, series_id),
            )
            if rows and len(rows) >= 5:
                vals = np.array([float(r["value"]) for r in rows])
                mean_change = float(np.mean(np.abs(np.diff(vals))))
                # Clamp: >5 pp/yr change = highly adaptive
                change_score = min(1.0, mean_change / 5.0)
                change_scores.append(change_score)
                sector_dates[label] = f"{rows[0]['date']} to {rows[-1]['date']}"

        # --- Governance quality ---
        gov_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GE.EST'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not change_scores and not gov_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        # GE.EST: -2.5 (weak) to +2.5 (strong). Normalize to 0-1 good governance.
        if gov_rows:
            ge_val = float(gov_rows[0]["value"])
            gov_effectiveness_norm = (ge_val + 2.5) / 5.0
            gov_effectiveness_norm = max(0.0, min(1.0, gov_effectiveness_norm))
            gov_date = gov_rows[0]["date"]
        else:
            gov_effectiveness_norm = 0.5
            gov_date = None

        mean_change_score = float(np.mean(change_scores)) if change_scores else 0.5

        # Stress = low change capacity + poor governance
        change_stress = 1.0 - mean_change_score
        gov_stress = 1.0 - gov_effectiveness_norm
        composite_stress = 0.5 * change_stress + 0.5 * gov_stress
        score = float(min(100.0, max(0.0, composite_stress * 100.0)))

        return {
            "score": round(score, 1),
            "country": country,
            "structural_change_capacity_norm": round(mean_change_score, 4),
            "governance_effectiveness_norm": round(gov_effectiveness_norm, 4),
            "governance_effectiveness_raw_ge_est": round(ge_val, 4) if gov_rows else None,
            "governance_date": gov_date,
            "sector_periods": sector_dates,
            "interpretation": (
                "High score = low adaptive capacity (slow structural change + poor governance). "
                "Low score = high adaptability."
            ),
            "_citation": "World Bank WDI: NV.AGR.TOTL.ZS, NV.IND.TOTL.ZS, GE.EST",
        }
