"""Disposable Income Trend module.

Tracks real disposable income trajectory by subtracting inflation from
GDP per capita growth. Persistent negative real income growth signals
households losing purchasing power despite nominal gains.

real_income_growth = gdp_growth - inflation
Score = clip(50 - real_income_growth * 5, 0, 100).

Sources: WDI (NY.GDP.PCAP.KD.ZG, FP.CPI.TOTL.ZG)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class DisposableIncomeTrend(LayerBase):
    layer_id = "lID"
    name = "Disposable Income Trend"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        gdp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.PCAP.KD.ZG'
            ORDER BY dp.date
            """,
            (country,),
        )

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

        if not gdp_rows or len(gdp_rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        gdp_map = {r["date"]: float(r["value"]) for r in gdp_rows}

        if cpi_rows and len(cpi_rows) >= 5:
            cpi_map = {r["date"]: float(r["value"]) for r in cpi_rows}
            common = sorted(set(gdp_map) & set(cpi_map))
            if len(common) < 5:
                # Use all gdp and average cpi
                common = None
        else:
            common = None

        if common and len(common) >= 5:
            gdp_vals = np.array([gdp_map[d] for d in common])
            cpi_vals = np.array([cpi_map[d] for d in common])
            real_income = gdp_vals - cpi_vals
            period = f"{common[0]} to {common[-1]}"
            method = "direct"
        else:
            # Use GDP per capita growth in constant prices (already inflation-adjusted)
            dates = sorted(gdp_map)
            gdp_vals = np.array([gdp_map[d] for d in dates])
            real_income = gdp_vals  # constant prices series is already real
            period = f"{dates[0]} to {dates[-1]}"
            method = "constant_price_proxy"

        mean_real_growth = float(np.mean(real_income))
        recent_real_growth = float(np.mean(real_income[-3:])) if len(real_income) >= 3 else mean_real_growth
        n_negative = int(np.sum(real_income < 0))

        # Trend
        t = np.arange(len(real_income))
        slope = float(np.polyfit(t, real_income, 1)[0]) if len(real_income) >= 3 else 0.0

        score = float(np.clip(50 - recent_real_growth * 5, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(real_income),
            "period": period,
            "method": method,
            "mean_real_income_growth_pct": round(mean_real_growth, 3),
            "recent_real_income_growth_pct": round(recent_real_growth, 3),
            "n_negative_years": n_negative,
            "trend_per_year": round(slope, 4),
            "interpretation": (
                "negative real income growth = falling purchasing power; "
                "score > 50 = stagnation or decline"
            ),
        }
