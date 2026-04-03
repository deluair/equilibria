"""Currency Crisis Risk module.

Currency crisis early warning based on Kaminsky-Reinhart (1999) signals approach.
Queries WDI:
  - FI.RES.TOTL.MO   : Total reserves in months of imports
  - BN.CAB.XOKA.GD.ZS : Current account balance as % of GDP

Crisis risk = low reserves + large current account deficit.
Thresholds (Kaminsky-Reinhart 1999, IMF guidance):
  - Reserves < 3 months of imports -> danger zone
  - CA deficit > 5% GDP -> external imbalance stress

Score formula:
  reserve_stress = clip((3 - reserves) / 3 * 60, 0, 60)
  ca_stress      = clip((-ca_balance) / 5 * 40, 0, 40)
  score          = reserve_stress + ca_stress

Sources: World Bank WDI, Kaminsky & Reinhart (1999) Journal of Finance.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CurrencyCrisisRisk(LayerBase):
    layer_id = "lRI"
    name = "Currency Crisis Risk"

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

        reserves = await fetch_mean("FI.RES.TOTL.MO")
        ca_balance = await fetch_mean("BN.CAB.XOKA.GD.ZS")

        if reserves is None and ca_balance is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no reserves or current account data",
            }

        reserve_stress = 0.0
        ca_stress = 0.0
        flags = []

        if reserves is not None:
            reserve_stress = float(np.clip((3.0 - reserves) / 3.0 * 60, 0, 60))
            if reserves < 3.0:
                flags.append(f"reserves below 3-month threshold: {reserves:.1f} months")

        if ca_balance is not None:
            # Negative ca_balance = deficit
            ca_stress = float(np.clip((-ca_balance) / 5.0 * 40, 0, 40))
            if ca_balance < -5.0:
                flags.append(f"CA deficit exceeds 5% GDP: {ca_balance:.1f}%")

        score = reserve_stress + ca_stress

        return {
            "score": round(score, 1),
            "country": country,
            "reserves_months": round(reserves, 2) if reserves is not None else None,
            "ca_balance_pct_gdp": round(ca_balance, 2) if ca_balance is not None else None,
            "components": {
                "reserve_stress": round(reserve_stress, 2),
                "ca_stress": round(ca_stress, 2),
            },
            "thresholds": {"reserves_months": 3.0, "ca_deficit_pct": 5.0},
            "flags": flags,
            "reference": "Kaminsky & Reinhart 1999 signals approach",
        }
