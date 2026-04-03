"""Border region trade: export partner concentration as border effect proxy.

Countries with exports concentrated in few partners exhibit a strong border
effect where geographic proximity dominates trade patterns. Low export partner
diversity signals border-region trade concentration and vulnerability to
partner-specific shocks.

Proxy: export growth rate (NX growth via NE.EXP.GNFS.ZS) combined with
merchandise export value (TX.VAL.MRCH.CD.WT). Concentration derived from
the ratio of export openness to import openness (asymmetry).

Score: export concentration index based on openness asymmetry and trend
volatility. High asymmetry = border concentration signal.

Score = clip(asymmetry_score + volatility_score, 0, 100)
  asymmetry_score = clip(abs(exp_share - imp_share) * 2, 0, 60)
  volatility_score: std dev of export growth / mean * 20, clipped 0-40.

References:
    McCallum, J. (1995). National Borders Matter: Canada-US Regional Trade
        Patterns. American Economic Review, 85(3), 615-623.
    Anderson, J.E. & van Wincoop, E. (2003). Gravity with Gravitas.
        American Economic Review, 93(1), 170-192.

Sources: World Bank WDI NE.EXP.GNFS.ZS, NE.IMP.GNFS.ZS, TX.VAL.MRCH.CD.WT.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class BorderRegionTrade(LayerBase):
    layer_id = "l11"
    name = "Border Region Trade"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        exp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.EXP.GNFS.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
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
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not exp_rows or not imp_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no export or import share data",
                "country": country,
            }

        exp_share = float(exp_rows[0]["value"])
        imp_share = float(imp_rows[0]["value"])
        year = exp_rows[0]["date"]

        asymmetry = abs(exp_share - imp_share)
        asymmetry_score = float(np.clip(asymmetry * 2.0, 0.0, 60.0))

        # Volatility of exports over time
        volatility_score = 0.0
        if len(exp_rows) >= 4:
            exp_vals = np.array([float(r["value"]) for r in exp_rows])
            exp_mean = float(np.mean(exp_vals))
            if exp_mean > 0:
                cv = float(np.std(exp_vals) / exp_mean)
                volatility_score = float(np.clip(cv * 20.0, 0.0, 40.0))

        score = float(np.clip(asymmetry_score + volatility_score, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "country": country,
            "export_share_gdp_pct": round(exp_share, 2),
            "import_share_gdp_pct": round(imp_share, 2),
            "trade_asymmetry_pp": round(asymmetry, 2),
            "volatility_cv": round(float(np.std([float(r["value"]) for r in exp_rows]) /
                                         max(1.0, float(np.mean([float(r["value"]) for r in exp_rows])))), 4)
                              if len(exp_rows) >= 4 else None,
            "year": year,
            "border_effect_level": (
                "high" if score > 60 else "moderate" if score > 30 else "low"
            ),
            "n_obs": len(exp_rows),
            "_source": "WDI NE.EXP.GNFS.ZS, NE.IMP.GNFS.ZS",
        }
