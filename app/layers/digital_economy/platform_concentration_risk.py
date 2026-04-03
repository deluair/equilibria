"""Platform Concentration Risk module.

Market concentration in digital platforms.
Proxy: NV.SRV.TOTL.ZS (services value added % GDP) combined with
IC.BUS.EASE.XQ (ease of doing business score) as structural indicators.

Low business environment openness + high services dominance without competitive checks
= elevated platform concentration risk.

Score: higher = greater concentration/structural risk.

Source: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PlatformConcentrationRisk(LayerBase):
    layer_id = "lDG"
    name = "Platform Concentration Risk"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        services_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NV.SRV.TOTL.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        ease_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IC.BUS.EASE.XQ'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not services_rows and not ease_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no services/business environment data"}

        services_vals = [float(r["value"]) for r in services_rows if r["value"] is not None]
        ease_vals = [float(r["value"]) for r in ease_rows if r["value"] is not None]

        services_mean = float(np.nanmean(services_vals)) if services_vals else None
        ease_mean = float(np.nanmean(ease_vals)) if ease_vals else None

        # Services dominance: higher = more concentrated digital services
        services_norm = float(np.clip((services_mean or 0) / 80.0 * 100, 0, 100)) if services_mean is not None else 50.0
        # Ease of business: higher = lower concentration risk (invert)
        ease_norm = float(np.clip(100 - (ease_mean or 50), 0, 100)) if ease_mean is not None else 50.0

        components = [services_norm, ease_norm]
        weights = [0.4, 0.6]
        risk = sum(c * w for c, w in zip(components, weights))
        score = float(np.clip(risk, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "services_va_pct_gdp": round(services_mean, 2) if services_mean is not None else None,
            "ease_of_business_score": round(ease_mean, 2) if ease_mean is not None else None,
            "note": "Higher score = greater platform concentration risk. Ease-of-business inverted.",
            "_citation": "World Bank WDI: NV.SRV.TOTL.ZS, IC.BUS.EASE.XQ",
        }
