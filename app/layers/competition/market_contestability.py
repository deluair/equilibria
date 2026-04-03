"""Market Contestability module.

Measures how contestable markets are using two proxies:
1. New business density (IC.BUS.NDNS.ZS): new registrations per 1,000 working-age
   population. Low density = few entrants = low contestability.
2. FDI inflows as % of GDP (BX.KLT.DINV.WD.GD.ZS): foreign entry signals that
   domestic markets are open and competitive.

Baumol et al. (1982): a contestable market has free entry/exit so incumbents
price at cost even without many actual competitors.

Score logic: low new business density + low FDI = high score (low contestability).
  biz_component = clip(50 - density * 5, 0, 50)
  fdi_component = clip(25 - fdi * 5, 0, 25) + 25 if fdi < 0
  score = clip(biz_component + fdi_component, 0, 100)

Sources: WDI (IC.BUS.NDNS.ZS, BX.KLT.DINV.WD.GD.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MarketContestability(LayerBase):
    layer_id = "lCO"
    name = "Market Contestability"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        density_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IC.BUS.NDNS.ZS'
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

        if not density_rows and not fdi_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no contestability data"}

        def latest_value(rows) -> float | None:
            for r in rows:
                if r["value"] is not None:
                    try:
                        return float(r["value"])
                    except (TypeError, ValueError):
                        pass
            return None

        density = latest_value(density_rows)
        fdi = latest_value(fdi_rows)

        # Low density = high barrier component
        biz_component = float(np.clip(50 - (density * 5 if density is not None else 50), 0, 50))

        # Low or negative FDI = less openness
        if fdi is not None:
            fdi_component = float(np.clip(25 - fdi * 5, 0, 50))
        else:
            fdi_component = 25.0  # neutral if missing

        score = float(np.clip(biz_component + fdi_component, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "new_business_density": round(density, 3) if density is not None else None,
            "fdi_pct_gdp": round(fdi, 3) if fdi is not None else None,
            "biz_component": round(biz_component, 2),
            "fdi_component": round(fdi_component, 2),
            "interpretation": (
                "highly contestable" if score < 33
                else "moderately contestable" if score < 66
                else "low contestability / protected markets"
            ),
            "reference": "Baumol, Panzar & Willig (1982): contestability theory",
        }
