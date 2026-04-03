"""Cybersecurity Risk module.

Cybersecurity risk proxy: digital exposure x governance quality.
  - IT.NET.USER.ZS: internet users % population (digital exposure)
  - GE.EST: government effectiveness (WGI, governance quality)

High internet use + poor governance = high cybersecurity risk.

Score = internet_norm * (1 - gov_norm), scaled to 0-100.

Source: World Bank WDI/WGI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CybersecurityRisk(LayerBase):
    layer_id = "lDF"
    name = "Cybersecurity Risk"

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

        if not internet_rows and not gov_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        internet_vals = [float(r["value"]) for r in internet_rows if r["value"] is not None]
        gov_vals = [float(r["value"]) for r in gov_rows if r["value"] is not None]

        internet_mean = float(np.nanmean(internet_vals)) if internet_vals else None
        gov_mean = float(np.nanmean(gov_vals)) if gov_vals else None

        # Normalize internet to [0, 1]
        internet_frac = float(np.clip((internet_mean or 0) / 100.0, 0, 1)) if internet_mean is not None else 0.5
        # Normalize governance from [-2.5, +2.5] to [0, 1] (higher = better)
        gov_frac = float(np.clip((gov_mean + 2.5) / 5.0, 0, 1)) if gov_mean is not None else 0.5

        # Risk = exposure * (1 - governance quality)
        risk = internet_frac * (1.0 - gov_frac)
        score = float(np.clip(risk * 100.0, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "internet_users_pct": round(internet_mean, 2) if internet_mean is not None else None,
            "gov_effectiveness_est": round(gov_mean, 4) if gov_mean is not None else None,
            "internet_frac": round(internet_frac, 4),
            "gov_frac": round(gov_frac, 4),
            "note": "Score 0 = low cybersecurity risk. Score 100 = high exposure with poor governance.",
            "_citation": "World Bank WDI: IT.NET.USER.ZS; WGI: GE.EST",
        }
