"""Border Region Development module.

Proxies the economic integration and development of border regions using
trade openness (exports + imports as % of GDP) and inward FDI flows.
Low openness combined with low FDI signals that border areas are not
benefiting from cross-border economic activity.

Score = clip(100 - (openness_norm + fdi_norm) / 2, 0, 100)
where openness_norm = min(trade_pct / 100, 1) * 100 (inverted)
      fdi_norm     = min(fdi_pct / 5, 1) * 100 (inverted)

Low trade + low FDI -> high score (stress).

Sources: WDI NE.TRD.GNFS.ZS (trade % of GDP),
         WDI BX.KLT.DINV.WD.GD.ZS (FDI net inflows % of GDP)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

# Benchmark: 100% trade openness, 5% FDI/GDP considered well-integrated
_TRADE_BENCHMARK = 100.0
_FDI_BENCHMARK = 5.0


class BorderRegionDevelopment(LayerBase):
    layer_id = "lRD"
    name = "Border Region Development"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows_trade = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.TRD.GNFS.ZS'
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

        if not rows_trade and not rows_fdi:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        trade_vals = [float(r["value"]) for r in rows_trade if r["value"] is not None]
        fdi_vals = [float(r["value"]) for r in rows_fdi if r["value"] is not None]

        components = {}
        penalty_parts = []

        if trade_vals:
            mean_trade = float(np.mean(trade_vals))
            openness_score = float(np.clip(mean_trade / _TRADE_BENCHMARK * 100, 0, 100))
            components["trade_openness"] = {
                "latest": round(trade_vals[0], 2),
                "mean": round(mean_trade, 2),
                "openness_score": round(openness_score, 2),
                "date": rows_trade[0]["date"],
            }
            penalty_parts.append(100 - openness_score)

        if fdi_vals:
            mean_fdi = float(np.mean(fdi_vals))
            fdi_score = float(np.clip(mean_fdi / _FDI_BENCHMARK * 100, 0, 100))
            components["fdi"] = {
                "latest": round(fdi_vals[0], 2),
                "mean": round(mean_fdi, 2),
                "fdi_score": round(fdi_score, 2),
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
                "trade_openness_pct": _TRADE_BENCHMARK,
                "fdi_pct_gdp": _FDI_BENCHMARK,
            },
            "series": {
                "trade": "NE.TRD.GNFS.ZS",
                "fdi": "BX.KLT.DINV.WD.GD.ZS",
            },
            "interpretation": "low openness + low FDI = underdeveloped border regions",
        }
