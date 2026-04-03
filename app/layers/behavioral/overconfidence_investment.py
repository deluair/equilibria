"""Overconfidence Investment module.

Investment overrun: GFCF growth vs GDP growth divergence.
Investment (gross fixed capital formation) consistently outpacing GDP growth
signals overconfidence in future returns.

Score based on mean spread (investment growth - GDP growth).

Sources: WDI NE.GDI.FTOT.ZS (GFCF % of GDP), NY.GDP.MKTP.KD.ZG (GDP growth)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class OverconfidenceInvestment(LayerBase):
    layer_id = "l13"
    name = "Overconfidence Investment"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        gfcf_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.GDI.FTOT.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        gdp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.MKTP.KD.ZG'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not gfcf_rows or len(gfcf_rows) < 5 or not gdp_rows or len(gdp_rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        # Align by date
        gfcf_map = {r["date"]: float(r["value"]) for r in gfcf_rows}
        gdp_map = {r["date"]: float(r["value"]) for r in gdp_rows}
        common_dates = sorted(set(gfcf_map) & set(gdp_map))

        if len(common_dates) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient overlapping dates"}

        gfcf_vals = np.array([gfcf_map[d] for d in common_dates])
        gdp_vals = np.array([gdp_map[d] for d in common_dates])

        # GFCF % of GDP changes as investment growth proxy
        gfcf_growth = np.diff(gfcf_vals)
        gdp_growth = gdp_vals[1:]

        spreads = gfcf_growth - gdp_growth
        mean_spread = float(np.mean(spreads))
        # Positive spread = investment outpacing GDP = overconfidence
        score = float(np.clip(max(0.0, mean_spread) * 5, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(common_dates),
            "period": f"{common_dates[0]} to {common_dates[-1]}",
            "mean_investment_gdp_spread": round(mean_spread, 3),
            "mean_gfcf_change": round(float(np.mean(gfcf_growth)), 3),
            "mean_gdp_growth": round(float(np.mean(gdp_growth)), 3),
            "interpretation": "Positive investment-GDP spread indicates overconfident capital allocation",
        }
