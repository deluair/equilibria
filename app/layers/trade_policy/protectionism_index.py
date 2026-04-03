"""Protectionism Index module.

Multi-dimensional protectionism composite combining tariff levels and
current account balance. A high applied tariff rate alongside a large
current account surplus (export-led mercantilist strategy) or persistent
deficit (structural import dependency despite restrictions) contributes
to the protectionism signal.

Score = clip(tariff_component * 0.6 + ca_component * 0.4, 0, 100)

Sources: WDI
  TM.TAX.MRCH.WM.AR.ZS - Tariff rate, applied, weighted mean (%)
  BN.CAB.XOKA.GD.ZS    - Current account balance (% of GDP)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ProtectionismIndex(LayerBase):
    layer_id = "lTP"
    name = "Protectionism Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        tariff_rows = await db.fetch_all(
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

        ca_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'BN.CAB.XOKA.GD.ZS'
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not tariff_rows and not ca_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no protectionism data available"}

        applied_tariff = None
        if tariff_rows:
            vals = [float(r["value"]) for r in tariff_rows if r["value"] is not None]
            if vals:
                applied_tariff = float(np.mean(vals[:5]))

        ca_balance = None
        if ca_rows:
            vals = [float(r["value"]) for r in ca_rows if r["value"] is not None]
            if vals:
                ca_balance = float(np.mean(vals[:5]))

        tariff_val = applied_tariff if applied_tariff is not None else 0.0
        ca_val = ca_balance if ca_balance is not None else 0.0

        # Tariff component: higher tariff -> higher protectionism
        tariff_component = float(np.clip(tariff_val * 5, 0, 100))

        # CA component: large absolute imbalance (either direction) signals trade tension
        ca_component = float(np.clip(abs(ca_val) * 3, 0, 100))

        if applied_tariff is None:
            score = ca_component
        elif ca_balance is None:
            score = tariff_component
        else:
            score = tariff_component * 0.6 + ca_component * 0.4

        regime = (
            "open" if score < 20
            else "moderately open" if score < 40
            else "moderately protectionist" if score < 60
            else "highly protectionist"
        )

        return {
            "score": round(score, 1),
            "country": country,
            "applied_tariff_pct": round(tariff_val, 2) if applied_tariff is not None else None,
            "current_account_pct_gdp": round(ca_val, 2) if ca_balance is not None else None,
            "tariff_component": round(tariff_component, 1),
            "ca_component": round(ca_component, 1),
            "protectionism_regime": regime,
        }
