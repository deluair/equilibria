"""Markup Estimation module.

Estimates price-cost markups using a value-added to labor productivity ratio
as a proxy. Higher industrial value added relative to labor productivity implies
firms capture rents above competitive pricing (market power).

Approach:
- Industry value added share (NV.IND.TOTL.ZS) captures the gross output
  minus intermediate inputs accruing to the sector.
- Labor productivity (SL.GDP.PCAP.EM.KD) is GDP per worker in constant USD.
- Markup proxy = value_added_share / (1 / labor_productivity_norm)
  where labor_productivity_norm rescales productivity to [0, 1] for comparability.
- Alternatively, the ratio of industry VA to GDP vs labour share of GDP
  approximates the profit share (markup = 1 + profit share / labour share).

Score = clip(markup_ratio * 50, 0, 100). High = high markups = market power stress.

Sources: WDI (NV.IND.TOTL.ZS, SL.GDP.PCAP.EM.KD)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MarkupEstimation(LayerBase):
    layer_id = "lCO"
    name = "Markup Estimation"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        va_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NV.IND.TOTL.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        lp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SL.GDP.PCAP.EM.KD'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not va_rows or not lp_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient markup data"}

        def latest_value(rows) -> float | None:
            for r in rows:
                if r["value"] is not None:
                    try:
                        return float(r["value"])
                    except (TypeError, ValueError):
                        pass
            return None

        va = latest_value(va_rows)
        lp = latest_value(lp_rows)

        if va is None or lp is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "missing markup inputs"}

        # Normalise labor productivity: higher lp -> lower unit labor cost proxy
        # Use log ratio as markup indicator; lp in USD thousands as baseline
        lp_norm = np.log1p(lp / 1000.0)  # log-scaled productivity

        # Markup proxy: industry VA share / normalised unit labor cost
        # Higher VA with moderate productivity -> rent extraction
        unit_labor_cost_proxy = 1.0 / max(lp_norm, 0.01)
        markup_ratio = (va / 100.0) / max(unit_labor_cost_proxy, 0.001)

        score = float(np.clip(markup_ratio * 50, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "industry_va_pct_gdp": round(va, 2),
            "labor_productivity_usd": round(lp, 1),
            "markup_ratio": round(markup_ratio, 4),
            "interpretation": (
                "competitive pricing" if score < 33
                else "moderate market power" if score < 66
                else "high markup / rent extraction"
            ),
            "reference": "De Loecker & Eeckhout (2017): markup = price / marginal cost",
        }
