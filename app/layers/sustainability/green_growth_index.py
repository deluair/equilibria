"""Green Growth Index: decoupling of GDP growth from CO2 emissions.

Measures the degree to which economic growth is decoupled from carbon emissions
using Pearson correlation between GDP growth rates and CO2 growth rates over a
rolling time window. Perfect decoupling (negative correlation) implies green
growth; strong coupling (positive correlation) signals unsustainable expansion.

Methodology:
    Compute year-over-year growth rates for GDP (constant USD) and CO2 (kt).
    Pearson correlation rho in [-1, 1]:
        rho = -1  -> perfect decoupling  -> score  0  (best)
        rho =  0  -> no relationship     -> score 50  (neutral)
        rho = +1  -> strong coupling     -> score 100 (worst)
    score = (rho + 1) / 2 * 100

References:
    OECD (2011). Towards Green Growth. OECD Publishing, Paris.
    Haberl, H. et al. (2020). "A systematic review of the evidence on decoupling."
        Environmental Research Letters, 15(6), 063003.
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class GreenGrowthIndex(LayerBase):
    layer_id = "lSU"
    name = "Green Growth Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('NY.GDP.MKTP.KD.ZG', 'EN.ATM.CO2E.KT')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 10:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient GDP/CO2 data for decoupling analysis"}

        series: dict[str, dict[str, float]] = {}
        for r in rows:
            sid = r["series_id"]
            yr = r["date"][:4] if isinstance(r["date"], str) else str(r["date"])
            series.setdefault(sid, {})[yr] = float(r["value"])

        gdp_g = series.get("NY.GDP.MKTP.KD.ZG", {})
        co2_kt = series.get("EN.ATM.CO2E.KT", {})

        # Derive CO2 growth rates from levels if needed
        co2_sorted = sorted(co2_kt.items())
        co2_growth: dict[str, float] = {}
        for i in range(1, len(co2_sorted)):
            prev_yr, prev_val = co2_sorted[i - 1]
            curr_yr, curr_val = co2_sorted[i]
            if prev_val and prev_val != 0:
                co2_growth[curr_yr] = (curr_val - prev_val) / abs(prev_val) * 100

        common_years = sorted(set(gdp_g.keys()) & set(co2_growth.keys()))
        if len(common_years) < 5:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient matched GDP/CO2 growth data"}

        gdp_arr = np.array([gdp_g[y] for y in common_years])
        co2_arr = np.array([co2_growth[y] for y in common_years])

        corr, pval = stats.pearsonr(gdp_arr, co2_arr)
        score = float(np.clip((corr + 1) / 2 * 100, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "n_years": len(common_years),
            "gdp_co2_correlation": round(float(corr), 4),
            "p_value": round(float(pval), 4),
            "decoupling_status": (
                "absolute_decoupling" if corr < -0.3 else
                "relative_decoupling" if corr < 0.1 else
                "coupled_growth"
            ),
            "year_range": [common_years[0], common_years[-1]],
        }
