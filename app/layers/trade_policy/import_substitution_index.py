"""Import Substitution Index module.

Measures import substitution effectiveness by jointly analysing trends
in manufacturing value added and import penetration. Rising imports
alongside declining manufacturing signals import substitution failure.

Score derived from combined trend signal:
  import_trend_component + manufacturing_decline_component

Sources: WDI
  NV.IND.MANF.ZS - Manufacturing, value added (% of GDP)
  NE.IMP.GNFS.ZS - Imports of goods and services (% of GDP)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class ImportSubstitutionIndex(LayerBase):
    layer_id = "lTP"
    name = "Import Substitution Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        manf_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NV.IND.MANF.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

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

        if not manf_rows and not imp_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no manufacturing or import data available",
            }

        manf_slope = None
        manf_mean = None
        if manf_rows:
            manf_vals = [float(r["value"]) for r in manf_rows if r["value"] is not None]
            if len(manf_vals) >= 5:
                manf_mean = float(np.mean(manf_vals))
                t = np.arange(len(manf_vals), dtype=float)
                manf_slope, *_ = linregress(t, manf_vals)
                manf_slope = float(manf_slope)

        imp_slope = None
        imp_mean = None
        if imp_rows:
            imp_vals = [float(r["value"]) for r in imp_rows if r["value"] is not None]
            if len(imp_vals) >= 5:
                imp_mean = float(np.mean(imp_vals))
                t = np.arange(len(imp_vals), dtype=float)
                imp_slope, *_ = linregress(t, imp_vals)
                imp_slope = float(imp_slope)

        # Import penetration rising -> higher stress
        imp_component = 0.0
        if imp_slope is not None:
            imp_component = float(np.clip(imp_slope * 10 + 20, 0, 60))

        # Manufacturing declining -> higher stress
        manf_component = 0.0
        if manf_slope is not None:
            manf_component = float(np.clip(-manf_slope * 15 + 20, 0, 40))

        if imp_slope is None and manf_slope is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient trend data"}

        score = float(np.clip(imp_component + manf_component, 0, 100))

        isi_assessment = (
            "substitution succeeding" if score < 25
            else "mixed results" if score < 50
            else "substitution stalling" if score < 75
            else "substitution failing"
        )

        return {
            "score": round(score, 1),
            "country": country,
            "manufacturing_pct_gdp_mean": round(manf_mean, 2) if manf_mean is not None else None,
            "manufacturing_trend_slope": round(manf_slope, 4) if manf_slope is not None else None,
            "import_pct_gdp_mean": round(imp_mean, 2) if imp_mean is not None else None,
            "import_trend_slope": round(imp_slope, 4) if imp_slope is not None else None,
            "isi_assessment": isi_assessment,
            "import_component": round(imp_component, 1),
            "manufacturing_component": round(manf_component, 1),
        }
