"""Digital Banking Depth module.

Banking depth + digital readiness proxy:
  - FS.AST.PRVT.GD.ZS: domestic credit to private sector % GDP (WDI)
  - IT.NET.USER.ZS: internet users % population (WDI)

Low private credit depth + low internet = shallow digital banking.

Score = 100 - depth_composite, where depth_composite combines
credit depth (normalized, cap 200% GDP) and internet penetration.

Source: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class DigitalBankingDepth(LayerBase):
    layer_id = "lDF"
    name = "Digital Banking Depth"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        credit_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FS.AST.PRVT.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

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

        if not credit_rows and not internet_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        credit_vals = [float(r["value"]) for r in credit_rows if r["value"] is not None]
        internet_vals = [float(r["value"]) for r in internet_rows if r["value"] is not None]

        credit_mean = float(np.nanmean(credit_vals)) if credit_vals else None
        internet_mean = float(np.nanmean(internet_vals)) if internet_vals else None

        # Normalize credit: cap at 200% GDP
        credit_norm = float(np.clip((credit_mean or 0) / 200.0 * 100, 0, 100)) if credit_mean is not None else 50.0
        internet_norm = float(np.clip(internet_mean or 0, 0, 100)) if internet_mean is not None else 50.0

        weights = []
        components = []
        if credit_mean is not None:
            weights.append(0.5)
            components.append(credit_norm)
        if internet_mean is not None:
            weights.append(0.5)
            components.append(internet_norm)

        total_w = sum(weights)
        depth_composite = sum(c * w for c, w in zip(components, weights)) / total_w if total_w > 0 else 50.0
        score = float(np.clip(100.0 - depth_composite, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "private_credit_pct_gdp": round(credit_mean, 2) if credit_mean is not None else None,
            "internet_users_pct": round(internet_mean, 2) if internet_mean is not None else None,
            "credit_norm": round(credit_norm, 2),
            "internet_norm": round(internet_norm, 2),
            "depth_composite": round(depth_composite, 2),
            "note": "Score 0 = deep digital banking. Score 100 = shallow.",
            "_citation": "World Bank WDI: FS.AST.PRVT.GD.ZS, IT.NET.USER.ZS",
        }
