"""Sovereign Default Risk module.

External debt sustainability analysis based on two WDI indicators:
  - DT.DOD.DECT.GD.ZS : External debt stocks as % of GNI (proxy for % GDP)
  - DT.TDS.DECT.GD.ZS : Total debt service as % of GNI

Thresholds (IMF/World Bank benchmarks):
  - Debt > 60% GDP -> elevated risk
  - Debt service > 15% GDP -> elevated risk

Score formula:
  score = clip(debt_gdp * 0.8 + debt_service * 3, 0, 100)

Sources: World Bank WDI, IMF Debt Sustainability Framework.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class SovereignDefaultRisk(LayerBase):
    layer_id = "lRI"
    name = "Sovereign Default Risk"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        async def fetch_latest(series_id: str, n: int = 5) -> float | None:
            rows = await db.fetch_all(
                """
                SELECT dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                  AND dp.value IS NOT NULL
                ORDER BY dp.date DESC
                LIMIT ?
                """,
                (country, series_id, n),
            )
            if not rows:
                return None
            vals = [float(r["value"]) for r in rows]
            return float(np.mean(vals))

        debt_gdp = await fetch_latest("DT.DOD.DECT.GD.ZS")
        debt_service = await fetch_latest("DT.TDS.DECT.GD.ZS")

        if debt_gdp is None and debt_service is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no debt data available",
            }

        # Use 0 for missing components (conservative)
        d = debt_gdp if debt_gdp is not None else 0.0
        ds = debt_service if debt_service is not None else 0.0

        score = float(np.clip(d * 0.8 + ds * 3.0, 0, 100))

        flags = []
        if debt_gdp is not None and debt_gdp > 60:
            flags.append(f"external debt {debt_gdp:.1f}% GDP exceeds 60% threshold")
        if debt_service is not None and debt_service > 15:
            flags.append(f"debt service {debt_service:.1f}% GDP exceeds 15% threshold")

        return {
            "score": round(score, 1),
            "country": country,
            "debt_gdp_pct": round(debt_gdp, 2) if debt_gdp is not None else None,
            "debt_service_gdp_pct": round(debt_service, 2) if debt_service is not None else None,
            "thresholds": {"debt_gdp": 60.0, "debt_service": 15.0},
            "flags": flags,
        }
