"""Tax Buoyancy module.

Estimates tax revenue elasticity to GDP growth. A buoyancy coefficient below 1
implies that tax revenues grow slower than the economy, signalling a shrinking
or eroding tax base and weakening fiscal capacity.

Methodology:
- Query GC.TAX.TOTL.GD.ZS (tax revenue, % GDP).
- Query NY.GDP.MKTP.KD.ZG (GDP growth, annual %).
- Compute period-over-period % changes in both series.
- Tax buoyancy = mean(% change in tax revenue) / mean(% change in GDP).
- Buoyancy < 1 -> declining tax base -> higher stress score.
- Score = clip(max(0, 1.5 - buoyancy) * 50, 0, 100).

Sources: World Bank WDI (GC.TAX.TOTL.GD.ZS, NY.GDP.MKTP.KD.ZG)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class TaxBuoyancy(LayerBase):
    layer_id = "lFP"
    name = "Tax Buoyancy"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        tax_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.TAX.TOTL.GD.ZS'
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

        if not tax_rows or not gdp_rows or len(tax_rows) < 4:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        # Align series by date
        tax_map = {r["date"]: float(r["value"]) for r in tax_rows}
        gdp_map = {r["date"]: float(r["value"]) for r in gdp_rows}
        common_dates = sorted(set(tax_map) & set(gdp_map))

        if len(common_dates) < 4:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient overlapping data"}

        tax_vals = np.array([tax_map[d] for d in common_dates])
        gdp_vals = np.array([gdp_map[d] for d in common_dates])

        # % changes in tax share
        tax_changes = np.diff(tax_vals) / (np.abs(tax_vals[:-1]) + 1e-10) * 100
        # GDP growth is already a % change
        gdp_changes = gdp_vals[1:]

        # Filter out zero GDP growth periods to avoid division artifacts
        mask = np.abs(gdp_changes) > 0.1
        if mask.sum() < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient variation"}

        mean_tax_change = float(np.mean(tax_changes[mask]))
        mean_gdp_change = float(np.mean(gdp_changes[mask]))

        if abs(mean_gdp_change) < 1e-10:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no gdp variation"}

        buoyancy = mean_tax_change / mean_gdp_change

        score = float(np.clip(max(0.0, 1.5 - buoyancy) * 50, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "tax_buoyancy": round(float(buoyancy), 4),
            "mean_tax_pct_change": round(mean_tax_change, 3),
            "mean_gdp_growth": round(mean_gdp_change, 3),
            "eroding_tax_base": buoyancy < 1.0,
            "n_obs": len(common_dates),
            "period": f"{common_dates[0]} to {common_dates[-1]}",
            "indicators": ["GC.TAX.TOTL.GD.ZS", "NY.GDP.MKTP.KD.ZG"],
        }
