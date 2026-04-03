"""Debt-Disaster Trap module.

Measures the risk that high external debt combined with high disaster exposure
creates a self-reinforcing spiral: disasters worsen fiscal positions, making
debt restructuring harder and future disaster response costlier.

Indicators:
  DT.DOD.DECT.GD.ZS -- External debt stocks (% of GNI)
  EN.CLC.MDAT.ZS    -- Population affected by droughts, floods, extreme temps (%)

Score = clip(debt_component + disaster_component, 0, 100)
  debt_component: clip(external_debt_pct / 2, 0, 60)
  disaster_component: clip(affected_pct * 1.5, 0, 40)

Sources: WDI (DT.DOD.DECT.GD.ZS, EN.CLC.MDAT.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class DebtDisasterTrap(LayerBase):
    layer_id = "lDE"
    name = "Debt Disaster Trap"

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

        debt_vals = await _fetch("DT.DOD.DECT.GD.ZS")
        disaster_vals = await _fetch("EN.CLC.MDAT.ZS")

        if not debt_vals and not disaster_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        external_debt_pct = float(np.mean(debt_vals)) if debt_vals else 40.0
        affected_pct = float(np.mean(disaster_vals)) if disaster_vals else 25.0

        debt_component = float(np.clip(external_debt_pct / 2.0, 0, 60))
        disaster_component = float(np.clip(affected_pct * 1.5, 0, 40))
        score = float(np.clip(debt_component + disaster_component, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "external_debt_pct_gni": round(external_debt_pct, 4),
            "disaster_exposure_pct": round(affected_pct, 4),
            "debt_component": round(debt_component, 2),
            "disaster_component": round(disaster_component, 2),
            "indicators": {
                "external_debt": "DT.DOD.DECT.GD.ZS",
                "disaster_exposure": "EN.CLC.MDAT.ZS",
            },
        }
