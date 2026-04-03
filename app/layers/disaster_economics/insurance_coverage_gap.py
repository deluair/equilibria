"""Insurance Coverage Gap module.

Measures the financial protection gap: countries with high disaster exposure
but shallow financial systems lack effective risk-transfer mechanisms.

Private credit depth (FS.AST.PRVT.GD.ZS) is used as a proxy for insurance
and financial protection penetration.

Score = clip(disaster_component + gap_component, 0, 100)
  disaster_component: affected_pct * 1.5
  gap_component: max(0, 100 - credit_depth) * 0.5

Sources: WDI (FS.AST.PRVT.GD.ZS, EN.CLC.MDAT.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class InsuranceCoverageGap(LayerBase):
    layer_id = "lDE"
    name = "Insurance Coverage Gap"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        async def _fetch(series_id: str) -> list[float]:
            rows = await db.fetch_all(
                """
                SELECT dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                ORDER BY dp.date DESC
                LIMIT 10
                """,
                (country, series_id),
            )
            return [float(r["value"]) for r in rows if r["value"] is not None]

        credit_vals = await _fetch("FS.AST.PRVT.GD.ZS")
        disaster_vals = await _fetch("EN.CLC.MDAT.ZS")

        if not credit_vals and not disaster_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        credit_depth = float(np.mean(credit_vals)) if credit_vals else 50.0
        affected_pct = float(np.mean(disaster_vals)) if disaster_vals else 25.0

        disaster_component = float(np.clip(affected_pct * 1.5, 0, 60))
        gap_component = float(np.clip(max(0.0, 100.0 - credit_depth) * 0.5, 0, 40))
        score = float(np.clip(disaster_component + gap_component, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "private_credit_pct_gdp": round(credit_depth, 4),
            "disaster_exposure_pct": round(affected_pct, 4),
            "disaster_component": round(disaster_component, 2),
            "gap_component": round(gap_component, 2),
            "indicators": {
                "private_credit": "FS.AST.PRVT.GD.ZS",
                "disaster_exposure": "EN.CLC.MDAT.ZS",
            },
        }
