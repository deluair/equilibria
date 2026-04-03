"""Tariff Regime module.

Measures applied tariff rate level and trend as a proxy for trade policy
protectionism. High applied tariffs signal restrictive trade regime.

Score = clip(applied_tariff * 5, 0, 100)

Sources: WDI
  TM.TAX.MRCH.WM.AR.ZS - Tariff rate, applied, weighted mean, all products (%)
  TM.TAX.MRCH.SM.AR.ZS - Tariff rate, applied, simple mean, all products (%)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class TariffRegime(LayerBase):
    layer_id = "lTP"
    name = "Tariff Regime"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        applied_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'TM.TAX.MRCH.WM.AR.ZS'
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        bound_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'TM.TAX.MRCH.SM.AR.ZS'
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not applied_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no applied tariff data"}

        applied_values = [float(r["value"]) for r in applied_rows if r["value"] is not None]
        if not applied_values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "all applied tariff values null"}

        applied_tariff = float(np.mean(applied_values[:5]))  # recent 5-year average

        bound_tariff = None
        if bound_rows:
            bound_values = [float(r["value"]) for r in bound_rows if r["value"] is not None]
            if bound_values:
                bound_tariff = float(np.mean(bound_values[:5]))

        score = float(np.clip(applied_tariff * 5, 0, 100))

        policy_stance = (
            "liberal" if applied_tariff < 5
            else "moderate" if applied_tariff < 10
            else "restrictive" if applied_tariff < 20
            else "highly protectionist"
        )

        result = {
            "score": round(score, 1),
            "country": country,
            "applied_tariff_pct": round(applied_tariff, 2),
            "policy_stance": policy_stance,
            "n_obs": len(applied_values),
        }
        if bound_tariff is not None:
            result["bound_tariff_pct"] = round(bound_tariff, 2)
            result["binding_overhang"] = round(bound_tariff - applied_tariff, 2)

        return result
