"""Public Goods Provision module.

Free-rider problem analysis via tax revenue vs public spending gap
(Samuelson 1954, Olson 1965).

Low tax/GDP + low spend/GDP = underprovision stress.
Score penalizes both low tax effort and low government expenditure
relative to typical public goods thresholds.

Sources: WDI (GC.TAX.TOTL.GD.ZS, GC.XPN.TOTL.GD.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

# Benchmark thresholds (% of GDP) below which underprovision is flagged
_TAX_THRESHOLD = 15.0   # IMF minimum tax/GDP benchmark
_SPEND_THRESHOLD = 20.0  # Rough lower bound for adequate public goods spend


class PublicGoodsProvision(LayerBase):
    layer_id = "lGT"
    name = "Public Goods Provision"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        tax_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.TAX.TOTL.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        spend_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.XPN.TOTL.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not tax_rows or not spend_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data: need GC.TAX.TOTL.GD.ZS and GC.XPN.TOTL.GD.ZS",
            }

        tax_pct = float(np.mean([float(r["value"]) for r in tax_rows]))
        spend_pct = float(np.mean([float(r["value"]) for r in spend_rows]))

        # Penalty for shortfall below threshold, scaled to 50 points each
        tax_gap = max(0.0, _TAX_THRESHOLD - tax_pct)
        spend_gap = max(0.0, _SPEND_THRESHOLD - spend_pct)

        tax_penalty = float(np.clip((tax_gap / _TAX_THRESHOLD) * 50.0, 0.0, 50.0))
        spend_penalty = float(np.clip((spend_gap / _SPEND_THRESHOLD) * 50.0, 0.0, 50.0))

        score = float(np.clip(tax_penalty + spend_penalty, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "tax_pct_gdp": round(tax_pct, 3),
            "spend_pct_gdp": round(spend_pct, 3),
            "tax_threshold": _TAX_THRESHOLD,
            "spend_threshold": _SPEND_THRESHOLD,
            "tax_gap": round(tax_gap, 3),
            "spend_gap": round(spend_gap, 3),
            "n_tax_obs": len(tax_rows),
            "n_spend_obs": len(spend_rows),
            "interpretation": (
                "severe underprovision of public goods" if score > 60
                else "moderate underprovision" if score > 30
                else "adequate public goods provision"
            ),
        }
