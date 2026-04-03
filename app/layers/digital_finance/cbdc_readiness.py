"""CBDC Readiness module.

Central Bank Digital Currency readiness composite from:
  - IT.NET.USER.ZS: internet users % population (infrastructure)
  - FM.LBL.BMNY.GD.ZS: broad money % GDP (financial system depth)
  - GE.EST: government effectiveness estimate (governance, WGI)

Low composite => low CBDC readiness => high score (stress).
Score = 100 - readiness_composite.

Source: World Bank WDI/WGI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CBDCReadiness(LayerBase):
    layer_id = "lDF"
    name = "CBDC Readiness"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        internet_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IT.NET.USER.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        money_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FM.LBL.BMNY.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        gov_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GE.EST'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not internet_rows and not money_rows and not gov_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        internet_vals = [float(r["value"]) for r in internet_rows if r["value"] is not None]
        money_vals = [float(r["value"]) for r in money_rows if r["value"] is not None]
        gov_vals = [float(r["value"]) for r in gov_rows if r["value"] is not None]

        internet_mean = float(np.nanmean(internet_vals)) if internet_vals else None
        money_mean = float(np.nanmean(money_vals)) if money_vals else None
        gov_mean = float(np.nanmean(gov_vals)) if gov_vals else None

        # Normalize each to 0-100
        internet_norm = float(np.clip(internet_mean or 0, 0, 100)) if internet_mean is not None else 50.0
        # Broad money: cap at 300% GDP
        money_norm = float(np.clip((money_mean or 0) / 300.0 * 100, 0, 100)) if money_mean is not None else 50.0
        # GE from [-2.5, +2.5] to [0, 100]
        gov_norm = float(np.clip((gov_mean + 2.5) / 5.0 * 100, 0, 100)) if gov_mean is not None else 50.0

        weights = []
        components = []
        if internet_mean is not None:
            weights.append(0.4)
            components.append(internet_norm)
        if money_mean is not None:
            weights.append(0.3)
            components.append(money_norm)
        if gov_mean is not None:
            weights.append(0.3)
            components.append(gov_norm)

        total_w = sum(weights)
        readiness_composite = sum(c * w for c, w in zip(components, weights)) / total_w if total_w > 0 else 50.0
        score = float(np.clip(100.0 - readiness_composite, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "internet_users_pct": round(internet_mean, 2) if internet_mean is not None else None,
            "broad_money_pct_gdp": round(money_mean, 2) if money_mean is not None else None,
            "gov_effectiveness_est": round(gov_mean, 4) if gov_mean is not None else None,
            "internet_norm": round(internet_norm, 2),
            "money_norm": round(money_norm, 2),
            "gov_norm": round(gov_norm, 2),
            "readiness_composite": round(readiness_composite, 2),
            "note": "Score 0 = high CBDC readiness. Score 100 = not ready.",
            "_citation": "World Bank WDI: IT.NET.USER.ZS, FM.LBL.BMNY.GD.ZS; WGI: GE.EST",
        }
