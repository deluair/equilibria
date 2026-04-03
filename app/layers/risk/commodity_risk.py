"""Commodity Risk module.

Commodity export dependency risk: vulnerability to commodity price shocks.
Queries WDI:
  - TX.VAL.FUEL.ZS.UN  : Fuel exports as % of merchandise exports
  - TX.VAL.MMTL.ZS.UN  : Ores and metals exports as % of merchandise exports

High concentration in fuel and/or metals = high exposure to commodity price cycles.

Score = clip((fuel_share + metals_share) * 1.2, 0, 100)
  Example: 40% fuel + 20% metals = 60 * 1.2 = 72 (STRESS)
           80% fuel + 10% metals = 90 * 1.2 = 100+ -> clipped to 100 (CRISIS)

Sources: World Bank WDI, UNCTAD commodity dependency framework.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CommodityRisk(LayerBase):
    layer_id = "lRI"
    name = "Commodity Risk"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        async def fetch_mean(series_id: str, n: int = 5) -> float | None:
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
            return float(np.mean([float(r["value"]) for r in rows]))

        fuel = await fetch_mean("TX.VAL.FUEL.ZS.UN")
        metals = await fetch_mean("TX.VAL.MMTL.ZS.UN")

        if fuel is None and metals is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no commodity export share data",
            }

        fuel_val = fuel if fuel is not None else 0.0
        metals_val = metals if metals is not None else 0.0

        total_commodity_share = fuel_val + metals_val
        score = float(np.clip(total_commodity_share * 1.2, 0, 100))

        flags = []
        if total_commodity_share > 60:
            flags.append(f"high commodity dependency: {total_commodity_share:.1f}% of exports")
        if fuel_val > 50:
            flags.append(f"fuel-dominant export structure: {fuel_val:.1f}%")
        if metals_val > 30:
            flags.append(f"metals-dominant export structure: {metals_val:.1f}%")

        return {
            "score": round(score, 1),
            "country": country,
            "fuel_exports_pct": round(fuel_val, 2),
            "metals_exports_pct": round(metals_val, 2),
            "total_commodity_pct": round(total_commodity_share, 2),
            "flags": flags,
            "interpretation": "% of merchandise exports. >60% combined = highly commodity-dependent.",
        }
