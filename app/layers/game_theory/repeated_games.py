"""Repeated Games module.

Measures international cooperation via Folk Theorem logic (Friedman 1971,
Axelrod 1984): sustained cooperation (FDI + ODA flows) signals repeated-game
equilibria; simultaneous decline signals defection.

Score rises when both FDI inflows and ODA receipts trend downward,
indicating breakdown of cooperative equilibria in international relations.

Sources: WDI (BX.KLT.DINV.WD.GD.ZS, DT.ODA.ALLD.GD.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class RepeatedGames(LayerBase):
    layer_id = "lGT"
    name = "Repeated Games"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        fdi_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'BX.KLT.DINV.WD.GD.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        oda_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'DT.ODA.ALLD.GD.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not fdi_rows or len(fdi_rows) < 5:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data: need BX.KLT.DINV.WD.GD.ZS (min 5 obs)",
            }

        fdi_vals = np.array([float(r["value"]) for r in fdi_rows])
        fdi_dates = [r["date"] for r in fdi_rows]
        t_fdi = np.arange(len(fdi_vals), dtype=float)
        fdi_slope = float(np.polyfit(t_fdi, fdi_vals, 1)[0])
        fdi_mean = float(np.mean(fdi_vals))

        # FDI trend normalized: negative = declining cooperation
        fdi_growth = fdi_slope / max(abs(fdi_mean), 1e-10)
        fdi_penalty = float(np.clip(-fdi_growth * 300.0, 0.0, 50.0))

        if oda_rows and len(oda_rows) >= 5:
            oda_vals = np.array([float(r["value"]) for r in oda_rows])
            t_oda = np.arange(len(oda_vals), dtype=float)
            oda_slope = float(np.polyfit(t_oda, oda_vals, 1)[0])
            oda_mean = float(np.mean(oda_vals))
            oda_growth = oda_slope / max(abs(oda_mean), 1e-10)
            oda_penalty = float(np.clip(-oda_growth * 300.0, 0.0, 50.0))
            oda_mean_val = round(oda_mean, 3)
            n_oda = len(oda_rows)
        else:
            oda_penalty = 0.0
            oda_growth = None
            oda_mean_val = None
            n_oda = 0

        score = float(np.clip(fdi_penalty + oda_penalty, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "fdi_inflows_pct_gdp_mean": round(fdi_mean, 3),
            "fdi_trend_growth_rate": round(fdi_growth, 6),
            "oda_pct_gdp_mean": oda_mean_val,
            "oda_trend_growth_rate": round(oda_growth, 6) if oda_growth is not None else None,
            "n_fdi_obs": len(fdi_rows),
            "n_oda_obs": n_oda,
            "period_fdi": f"{fdi_dates[0]} to {fdi_dates[-1]}",
            "interpretation": (
                "defection from international cooperation: declining FDI and aid"
                if score > 60
                else "weakening cooperative ties" if score > 30
                else "stable international cooperation equilibrium"
            ),
        }
