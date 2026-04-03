"""Circular Economy: material efficiency measured as GDP per unit CO2 emissions.

A proxy for circular economy performance based on the ratio of real GDP (constant
USD) to CO2 emissions (kt). A rising GDP/CO2 ratio signals improving material
efficiency and resource productivity. A declining trend indicates the economy
is becoming less circular (more carbon-intensive per unit of output).

Methodology:
    eco_efficiency_t = GDP_t / CO2_t  (constant USD per kt CO2)
    Fit linear trend: eco_efficiency ~ beta0 + beta1 * t
    Normalize slope relative to mean level:
        relative_slope = beta1 / mean(eco_efficiency)
    score = clip(50 - relative_slope * 500, 0, 100)
        (declining efficiency -> score near 100; rising efficiency -> score near 0)

References:
    Ellen MacArthur Foundation (2013). Towards the Circular Economy. Vol. 1.
    Ghisellini, P., Cialani, C. & Ulgiati, S. (2016). "A review on circular economy."
        Journal of Cleaner Production, 114, 11-32.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CircularEconomy(LayerBase):
    layer_id = "lSU"
    name = "Circular Economy"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('NY.GDP.MKTP.KD', 'EN.ATM.CO2E.KT')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 8:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient GDP/CO2 data for circular economy analysis"}

        series: dict[str, dict[str, float]] = {}
        for r in rows:
            sid = r["series_id"]
            yr = r["date"][:4] if isinstance(r["date"], str) else str(r["date"])
            series.setdefault(sid, {})[yr] = float(r["value"])

        gdp = series.get("NY.GDP.MKTP.KD", {})
        co2 = series.get("EN.ATM.CO2E.KT", {})

        common_years = sorted(set(gdp.keys()) & set(co2.keys()))
        if len(common_years) < 5:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient matched GDP/CO2 data"}

        years = np.array([int(y) for y in common_years])
        gdp_arr = np.array([gdp[y] for y in common_years])
        co2_arr = np.array([co2[y] for y in common_years])

        # Avoid division by zero
        nonzero = co2_arr != 0
        if nonzero.sum() < 5:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "CO2 data insufficient (zeros)"}

        eco_eff = gdp_arr[nonzero] / co2_arr[nonzero]
        t = years[nonzero] - years[nonzero][0]
        slope, intercept = np.polyfit(t, eco_eff, 1)

        mean_eff = float(np.mean(eco_eff))
        relative_slope = float(slope) / mean_eff if mean_eff != 0 else 0.0

        # Declining efficiency (negative slope) -> high score (bad)
        score = float(np.clip(50 - relative_slope * 500, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "n_years": int(nonzero.sum()),
            "eco_efficiency": {
                "latest_gdp_per_ktco2": round(float(eco_eff[-1]), 2),
                "mean_gdp_per_ktco2": round(mean_eff, 2),
                "trend_slope": round(float(slope), 4),
                "relative_slope": round(relative_slope, 6),
                "direction": "improving" if slope > 0 else "declining",
            },
            "year_range": [common_years[0], common_years[-1]],
        }
