"""Backward Linkages module.

Measures import intensity of exports as a proxy for backward GVC linkages.

A country with strong backward linkages imports intermediate inputs for
re-export as processed goods. Operationalized as:

  ratio = (imports % GDP) / (exports % GDP)

High ratio (close to or above 1) signals that import intensity matches or
exceeds export volume, indicating deep integration with upstream suppliers.
A rising ratio over time reinforces the finding.

Score is anchored around a neutral ratio of 0.7:
  - ratio > 1.2: very high backward linkage (low stress, score near 0)
  - ratio < 0.3: weak linkage (high stress, score near 100)

Sources: World Bank WDI (NE.IMP.GNFS.ZS, NE.EXP.GNFS.ZS).
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class BackwardLinkages(LayerBase):
    layer_id = "lVC"
    name = "Backward Linkages"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        imp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.IMP.GNFS.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        exp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.EXP.GNFS.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not imp_rows or not exp_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient trade data"}

        imp_vals = np.array([float(r["value"]) for r in imp_rows])
        exp_vals = np.array([float(r["value"]) for r in exp_rows])

        min_len = min(len(imp_vals), len(exp_vals))
        if min_len < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient aligned data"}

        imp_vals = imp_vals[-min_len:]
        exp_vals = exp_vals[-min_len:]

        # Avoid division by zero
        exp_vals_safe = np.where(exp_vals < 0.1, 0.1, exp_vals)
        ratios = imp_vals / exp_vals_safe

        mean_ratio = float(np.mean(ratios))
        latest_ratio = float(ratios[-1])

        # Trend in ratio
        trend_slope = None
        if min_len >= 4:
            x = np.arange(min_len, dtype=float)
            slope, _, _, _, _ = linregress(x, ratios)
            trend_slope = float(slope)

        # Score: high ratio = strong backward linkage = lower stress
        # ratio >= 1.2 -> score 0; ratio <= 0.3 -> score 100
        score = float(np.clip((1.2 - mean_ratio) / (1.2 - 0.3) * 100, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "mean_import_export_ratio": round(mean_ratio, 3),
            "latest_ratio": round(latest_ratio, 3),
            "trend_slope_per_yr": round(trend_slope, 5) if trend_slope is not None else None,
            "mean_imports_pct_gdp": round(float(np.mean(imp_vals)), 2),
            "mean_exports_pct_gdp": round(float(np.mean(exp_vals)), 2),
            "n_obs": min_len,
            "interpretation": (
                "strong backward GVC linkage" if mean_ratio >= 1.0
                else "moderate backward linkage" if mean_ratio >= 0.6
                else "weak backward linkage"
            ),
        }
