"""Sanctions Impact module.

Sanctions proxy: simultaneous contraction in exports and FDI inflows signals
economic isolation consistent with sanctions exposure or geopolitical punishment
(Hufbauer et al. 2007; Neuenkirch & Neumeier 2015). Either alone may reflect
the business cycle; both negative together is the isolation signal.

Score = isolation_signal_strength, based on joint negative co-occurrence.

Sources: WDI (NE.EXP.GNFS.KD.ZG export volume growth, BX.KLT.DINV.WD.GD.ZS FDI net inflows)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class SanctionsImpact(LayerBase):
    layer_id = "lIN"
    name = "Sanctions Impact"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        export_rows = await db.fetch_all(
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

        fdi_rows = await db.fetch_all(
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

        if not export_rows or not fdi_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient export or FDI data",
            }

        export_values = [float(r["value"]) for r in export_rows if r["value"] is not None]
        fdi_values = [float(r["value"]) for r in fdi_rows if r["value"] is not None]

        if not export_values or not fdi_values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no valid numeric data",
            }

        avg_export_growth = float(np.mean(export_values))
        avg_fdi = float(np.mean(fdi_values))

        # Isolation signal: both negative -> maximum stress
        export_negative = avg_export_growth < 0
        fdi_negative = avg_fdi < 0

        if export_negative and fdi_negative:
            # Severity scales with magnitude of joint contraction
            export_penalty = float(np.clip(abs(avg_export_growth) * 5, 0, 50))
            fdi_penalty = float(np.clip(abs(avg_fdi) * 10, 0, 50))
            score = export_penalty + fdi_penalty
        elif export_negative or fdi_negative:
            # Only one negative: partial stress
            if export_negative:
                score = float(np.clip(abs(avg_export_growth) * 3, 0, 40))
            else:
                score = float(np.clip(abs(avg_fdi) * 5, 0, 40))
        else:
            score = 0.0

        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "avg_export_growth_pct": round(avg_export_growth, 3),
            "avg_fdi_pct_gdp": round(avg_fdi, 3),
            "isolation_signal": export_negative and fdi_negative,
            "export_contracting": export_negative,
            "fdi_contracting": fdi_negative,
        }
