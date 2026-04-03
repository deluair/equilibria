"""Mechanism Design module.

Evaluates fiscal mechanism efficiency: persistent deficits despite
adequate tax revenue signal mechanism failure (Hurwicz 1972, Myerson 1979).

A well-designed fiscal mechanism converts tax effort into balanced or
surplus outcomes. Chronic deficits with high tax/GDP = incentive misalignment.

Score rises with: (a) large persistent deficits and (b) adequate tax base,
penalizing institutional failure rather than mere poverty.

Sources: WDI (GC.TAX.TOTL.GD.ZS, GC.BAL.CASH.GD.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_DEFICIT_THRESHOLD = -3.0   # % GDP; Maastricht-style threshold
_TAX_ADEQUATE = 12.0        # % GDP; minimum for "adequate" revenue base


class MechanismDesign(LayerBase):
    layer_id = "lGT"
    name = "Mechanism Design"

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
            LIMIT 15
            """,
            (country,),
        )

        balance_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.BAL.CASH.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 15
            """,
            (country,),
        )

        if not tax_rows or not balance_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data: need GC.TAX.TOTL.GD.ZS and GC.BAL.CASH.GD.ZS",
            }

        tax_vals = [float(r["value"]) for r in tax_rows]
        bal_vals = [float(r["value"]) for r in balance_rows]

        tax_mean = float(np.mean(tax_vals))
        bal_mean = float(np.mean(bal_vals))

        # Fraction of years with deficit below threshold
        deficit_years = sum(1 for v in bal_vals if v < _DEFICIT_THRESHOLD)
        deficit_persistence = deficit_years / len(bal_vals)

        # Severity: how far below threshold on average
        deficits_only = [v for v in bal_vals if v < _DEFICIT_THRESHOLD]
        deficit_depth = float(np.mean(deficits_only)) if deficits_only else 0.0
        depth_penalty = float(np.clip(abs(deficit_depth) / 10.0 * 40.0, 0.0, 40.0))

        # Adequacy of tax base (higher tax base = less excuse for deficits)
        tax_adequacy = float(np.clip(tax_mean / _TAX_ADEQUATE, 0.0, 1.0))

        # Mechanism failure score: persistence * depth * tax_adequacy_weight
        persistence_penalty = float(np.clip(deficit_persistence * 60.0, 0.0, 60.0))
        mechanism_penalty = persistence_penalty + depth_penalty * tax_adequacy

        score = float(np.clip(mechanism_penalty, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "tax_pct_gdp_mean": round(tax_mean, 3),
            "fiscal_balance_pct_gdp_mean": round(bal_mean, 3),
            "deficit_persistence": round(deficit_persistence, 4),
            "deficit_depth_mean": round(deficit_depth, 3),
            "tax_adequacy_ratio": round(tax_adequacy, 4),
            "n_tax_obs": len(tax_rows),
            "n_balance_obs": len(balance_rows),
            "interpretation": (
                "chronic mechanism failure: persistent deficits despite adequate revenue"
                if score > 60
                else "moderate fiscal mechanism stress" if score > 30
                else "functional fiscal mechanism"
            ),
        }
