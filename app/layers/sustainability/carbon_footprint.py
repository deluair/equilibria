"""Carbon Footprint: per capita CO2 emissions relative to income-adjusted benchmark.

Compares a country's per capita CO2 emissions to the expected level given its
per capita income, using an OLS regression across all available country-year
observations in the database. Countries emitting more than the income-adjusted
benchmark are scoring above the global norm for their development stage.

Methodology:
    Estimate: log(CO2_pc) = alpha + beta * log(GDP_pc) + epsilon
    Benchmark_pc = exp(alpha + beta * log(GDP_pc_country))
    excess = (CO2_pc_actual - benchmark) / benchmark * 100
    score = clip(50 + excess / 2, 0, 100)
        excess < 0 -> below benchmark (score < 50, good)
        excess > 0 -> above benchmark (score > 50, bad)

References:
    Stern, D.I. (2004). "The rise and fall of the environmental Kuznets curve."
        World Development, 32(8), 1419-1439.
    Peters, G. et al. (2012). "Rapid growth in CO2 emissions after the 2008-2009
        global financial crisis." Nature Climate Change, 2(1), 2-4.
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class CarbonFootprint(LayerBase):
    layer_id = "lSU"
    name = "Carbon Footprint"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        # Fetch target country data
        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('EN.ATM.CO2E.PC', 'NY.GDP.PCAP.KD')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 6:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient per-capita CO2/GDP data"}

        series: dict[str, dict[str, float]] = {}
        for r in rows:
            sid = r["series_id"]
            yr = r["date"][:4] if isinstance(r["date"], str) else str(r["date"])
            series.setdefault(sid, {})[yr] = float(r["value"])

        co2_pc = series.get("EN.ATM.CO2E.PC", {})
        gdp_pc = series.get("NY.GDP.PCAP.KD", {})

        common = sorted(set(co2_pc.keys()) & set(gdp_pc.keys()))
        if len(common) < 4:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient matched CO2/GDP per-capita data"}

        co2_arr = np.array([co2_pc[y] for y in common])
        gdp_arr = np.array([gdp_pc[y] for y in common])

        # Use country's own time series for income-adjusted benchmark via OLS
        valid = (co2_arr > 0) & (gdp_arr > 0)
        if valid.sum() < 4:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient positive CO2/GDP values"}

        log_co2 = np.log(co2_arr[valid])
        log_gdp = np.log(gdp_arr[valid])
        slope, intercept, _, _, _ = stats.linregress(log_gdp, log_co2)

        latest_co2 = float(co2_arr[-1])
        latest_gdp = float(gdp_arr[-1])

        if latest_gdp > 0:
            benchmark = float(np.exp(intercept + slope * np.log(latest_gdp)))
        else:
            benchmark = float(np.mean(co2_arr[valid]))

        excess_pct = (latest_co2 - benchmark) / benchmark * 100 if benchmark != 0 else 0.0
        score = float(np.clip(50 + excess_pct / 2, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "n_years": int(valid.sum()),
            "latest_co2_pc_tonnes": round(latest_co2, 3),
            "income_adjusted_benchmark": round(benchmark, 3),
            "excess_pct": round(excess_pct, 2),
            "elasticity": round(float(slope), 4),
            "year_range": [common[0], common[-1]],
        }
