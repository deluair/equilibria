"""Housing Price Index module.

Proxies housing price overheating by comparing CPI inflation to GDP growth.
Sustained CPI outpacing GDP growth signals real asset overvaluation.

Queries:
- FP.CPI.TOTL.ZG: CPI inflation (%)
- NY.GDP.MKTP.KD.ZG: GDP growth (%)

Score based on frequency and magnitude of years where CPI > GDP growth.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class HousingPriceIndex(LayerBase):
    layer_id = "lRE"
    name = "Housing Price Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        cpi_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FP.CPI.TOTL.ZG'
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

        if not cpi_rows or len(cpi_rows) < 3 or not gdp_rows or len(gdp_rows) < 3:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient CPI or GDP data for housing price index analysis",
            }

        # Align by date
        cpi_dict = {r["date"]: float(r["value"]) for r in cpi_rows}
        gdp_dict = {r["date"]: float(r["value"]) for r in gdp_rows}
        common_dates = sorted(set(cpi_dict.keys()) & set(gdp_dict.keys()))

        if len(common_dates) < 3:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient overlapping CPI/GDP observations",
            }

        cpi_arr = np.array([cpi_dict[d] for d in common_dates])
        gdp_arr = np.array([gdp_dict[d] for d in common_dates])

        # Price-growth gap: CPI - GDP growth
        gap = cpi_arr - gdp_arr

        # Overheating: frequency of years CPI > GDP, weighted by magnitude
        overheat_mask = gap > 0
        overheat_freq = float(np.mean(overheat_mask))
        avg_gap_when_positive = float(np.mean(gap[overheat_mask])) if overheat_mask.any() else 0.0

        # Recent overheating carries extra weight (last 3 years)
        recent_gap = gap[-3:]
        recent_overheat = float(np.mean(recent_gap > 0))

        raw_score = (overheat_freq * 40) + (avg_gap_when_positive * 3) + (recent_overheat * 20)
        score = float(np.clip(raw_score, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "cpi_latest_pct": round(float(cpi_arr[-1]), 2),
            "gdp_growth_latest_pct": round(float(gdp_arr[-1]), 2),
            "price_growth_gap_latest": round(float(gap[-1]), 2),
            "overheat_frequency": round(overheat_freq, 3),
            "avg_gap_when_positive": round(avg_gap_when_positive, 2),
            "recent_overheat_frequency": round(recent_overheat, 3),
            "n_obs": len(common_dates),
            "period": f"{common_dates[0]} to {common_dates[-1]}",
            "methodology": "overheat freq * 40 + avg positive gap * 3 + recent overheat * 20",
        }
