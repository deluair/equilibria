"""Commodity Cycle Synchrony module.

Measures the degree to which a country's economic cycle is aligned with
global commodity price cycles. High synchrony amplifies external shocks
and reduces policy autonomy.

Methodology:
- Query GDP growth (NY.GDP.MKTP.KD.ZG) for the country.
- Query a global commodity price index proxy (PBCOM_USD or similar WLD series).
- Compute Pearson correlation over aligned annual windows (min 10 obs).
- Synchrony score = clip((correlation + 1) / 2 * 100, 0, 100).
  correlation = +1 (perfectly pro-cyclical) -> score 100 (most vulnerable).
  correlation = -1 (counter-cyclical) -> score 0 (most resilient).

Sources: World Bank WDI (NY.GDP.MKTP.KD.ZG), IMF/World Bank commodity indices.
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class CommodityCycleSynchrony(LayerBase):
    layer_id = "lCM"
    name = "Commodity Cycle Synchrony"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        async def _series(series_id: str, iso3: str) -> dict[str, float]:
            rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                ORDER BY dp.date
                """,
                (iso3, series_id),
            )
            return {row["date"]: float(row["value"]) for row in rows}

        gdp_growth = await _series("NY.GDP.MKTP.KD.ZG", country)
        commodity_px = await _series("PBCOM_USD", "WLD")

        if not gdp_growth or not commodity_px:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data for cycle comparison"}

        # Align on common dates
        common = sorted(set(gdp_growth) & set(commodity_px))
        if len(common) < 10:
            return {"score": None, "signal": "UNAVAILABLE", "error": "fewer than 10 overlapping observations"}

        g = np.array([gdp_growth[d] for d in common])
        c = np.array([commodity_px[d] for d in common])

        # Year-over-year change in commodity index
        c_growth = np.diff(c) / np.abs(c[:-1] + 1e-10) * 100
        g_aligned = g[1:]

        if len(g_aligned) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient growth observations"}

        corr, pval = sp_stats.pearsonr(g_aligned, c_growth)
        score = float(np.clip((corr + 1) / 2 * 100, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "correlation": round(float(corr), 4),
            "pvalue": round(float(pval), 4),
            "n_obs": len(g_aligned),
            "high_synchrony": corr > 0.5,
            "indicators": ["NY.GDP.MKTP.KD.ZG", "PBCOM_USD"],
        }
