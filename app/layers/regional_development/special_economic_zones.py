"""Special Economic Zones module.

Proxies SEZ and investment-zone effectiveness by examining whether export
growth and FDI inflows are jointly positive and trending upward. Both
declining simultaneously signals that SEZ-style interventions are failing
to attract investment and generate export activity.

Sub-scores:
  export_score = clip((export_growth + 5) * 5, 0, 100)  -- maps -5% -> 0, +15% -> 100
  fdi_score    = clip(fdi_pct / 5 * 100, 0, 100)        -- 5% FDI/GDP = full score

Score = 100 - (export_score + fdi_score) / 2
High when both export growth and FDI are low/negative.

Sources: WDI NE.EXP.GNFS.KD.ZG (exports of goods and services % growth),
         WDI BX.KLT.DINV.WD.GD.ZS (FDI net inflows % of GDP)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_FDI_BENCHMARK = 5.0  # % of GDP treated as strong FDI attraction


class SpecialEconomicZones(LayerBase):
    layer_id = "lRD"
    name = "Special Economic Zones"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows_exp = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.EXP.GNFS.KD.ZG'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        rows_fdi = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'BX.KLT.DINV.WD.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not rows_exp and not rows_fdi:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        exp_vals = [float(r["value"]) for r in rows_exp if r["value"] is not None]
        fdi_vals = [float(r["value"]) for r in rows_fdi if r["value"] is not None]

        penalty_parts = []
        components = {}

        if exp_vals:
            mean_exp = float(np.mean(exp_vals))
            export_score = float(np.clip((mean_exp + 5) * 5, 0, 100))
            components["export_growth"] = {
                "latest": round(exp_vals[0], 2),
                "mean": round(mean_exp, 2),
                "sub_score": round(export_score, 2),
                "date": rows_exp[0]["date"],
            }
            penalty_parts.append(100 - export_score)

        if fdi_vals:
            mean_fdi = float(np.mean(fdi_vals))
            fdi_score = float(np.clip(mean_fdi / _FDI_BENCHMARK * 100, 0, 100))
            components["fdi"] = {
                "latest": round(fdi_vals[0], 2),
                "mean": round(mean_fdi, 2),
                "sub_score": round(fdi_score, 2),
                "date": rows_fdi[0]["date"],
            }
            penalty_parts.append(100 - fdi_score)

        if not penalty_parts:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid values"}

        score = float(np.clip(np.mean(penalty_parts), 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "components": components,
            "benchmarks": {
                "export_growth_target_pct": 15.0,
                "fdi_pct_gdp_target": _FDI_BENCHMARK,
            },
            "series": {
                "export_growth": "NE.EXP.GNFS.KD.ZG",
                "fdi": "BX.KLT.DINV.WD.GD.ZS",
            },
            "interpretation": "both declining = SEZ/investment zone failure",
        }
