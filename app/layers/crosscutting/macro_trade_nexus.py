"""Macro-Trade Nexus module.

Trade openness x GDP growth feedback loop (Frankel & Romer 1999).

Queries trade openness (NE.TRD.GNFS.ZS, % of GDP) and GDP growth
(NY.GDP.MKTP.KD.ZG) over available history and computes their
Pearson correlation. Theory predicts a positive relationship: more
open economies tend to grow faster. A weak or negative correlation
signals decoupled or stressed macro-trade dynamics.

Score rises as the correlation weakens or turns negative (stress
when openness does not translate to growth gains).
"""

from __future__ import annotations

import numpy as np
from scipy.stats import pearsonr

from app.layers.base import LayerBase


class MacroTradeNexus(LayerBase):
    layer_id = "lCX"
    name = "Macro-Trade Nexus"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows_trade = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.TRD.GNFS.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        rows_gdp = await db.fetch_all(
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

        if not rows_trade or not rows_gdp:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data for trade openness or GDP growth",
            }

        trade_map = {r["date"]: float(r["value"]) for r in rows_trade if r["value"] is not None}
        gdp_map = {r["date"]: float(r["value"]) for r in rows_gdp if r["value"] is not None}

        common_dates = sorted(set(trade_map) & set(gdp_map))
        if len(common_dates) < 8:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"only {len(common_dates)} overlapping observations (need 8+)",
            }

        trade_vals = np.array([trade_map[d] for d in common_dates])
        gdp_vals = np.array([gdp_map[d] for d in common_dates])

        corr, p_value = pearsonr(trade_vals, gdp_vals)

        # Score: correlation of +1.0 -> 0 stress; correlation of -1.0 -> 100 stress
        # Linear mapping: score = (1 - corr) / 2 * 100
        score = float(np.clip((1.0 - corr) / 2.0 * 100.0, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(common_dates),
            "period": f"{common_dates[0]} to {common_dates[-1]}",
            "correlation": round(float(corr), 4),
            "p_value": round(float(p_value), 4),
            "trade_openness_mean": round(float(np.mean(trade_vals)), 2),
            "gdp_growth_mean": round(float(np.mean(gdp_vals)), 2),
            "interpretation": (
                "positive feedback" if corr > 0.3
                else "weak nexus" if corr > 0.0
                else "decoupled/negative"
            ),
            "reference": "Frankel & Romer 1999, JEL D24",
        }
