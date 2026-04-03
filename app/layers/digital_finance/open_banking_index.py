"""Open Banking Index module.

Open banking readiness: private credit + regulatory quality + internet.
  - FS.AST.PRVT.GD.ZS: domestic credit to private sector % GDP
  - RQ.EST: regulatory quality estimate (WGI)
  - IT.NET.USER.ZS: internet users % population

Normalize and combine equally weighted. Low composite = closed banking system.

Score = 100 - open_banking_composite.

Source: World Bank WDI/WGI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class OpenBankingIndex(LayerBase):
    layer_id = "lDF"
    name = "Open Banking Index"

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

        rq_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'RQ.EST'
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

        if not credit_rows and not rq_rows and not internet_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        credit_vals = [float(r["value"]) for r in credit_rows if r["value"] is not None]
        rq_vals = [float(r["value"]) for r in rq_rows if r["value"] is not None]
        internet_vals = [float(r["value"]) for r in internet_rows if r["value"] is not None]

        credit_mean = float(np.nanmean(credit_vals)) if credit_vals else None
        rq_mean = float(np.nanmean(rq_vals)) if rq_vals else None
        internet_mean = float(np.nanmean(internet_vals)) if internet_vals else None

        credit_norm = float(np.clip((credit_mean or 0) / 200.0 * 100, 0, 100)) if credit_mean is not None else 50.0
        rq_norm = float(np.clip(((rq_mean or 0) + 2.5) / 5.0 * 100, 0, 100)) if rq_mean is not None else 50.0
        internet_norm = float(np.clip(internet_mean or 0, 0, 100)) if internet_mean is not None else 50.0

        weights = []
        components = []
        if credit_mean is not None:
            weights.append(1.0 / 3.0)
            components.append(credit_norm)
        if rq_mean is not None:
            weights.append(1.0 / 3.0)
            components.append(rq_norm)
        if internet_mean is not None:
            weights.append(1.0 / 3.0)
            components.append(internet_norm)

        total_w = sum(weights)
        open_banking_composite = sum(c * w for c, w in zip(components, weights)) / total_w if total_w > 0 else 50.0
        score = float(np.clip(100.0 - open_banking_composite, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "private_credit_pct_gdp": round(credit_mean, 2) if credit_mean is not None else None,
            "regulatory_quality_est": round(rq_mean, 4) if rq_mean is not None else None,
            "internet_users_pct": round(internet_mean, 2) if internet_mean is not None else None,
            "credit_norm": round(credit_norm, 2),
            "rq_norm": round(rq_norm, 2),
            "internet_norm": round(internet_norm, 2),
            "open_banking_composite": round(open_banking_composite, 2),
            "note": "Score 0 = highly open banking system. Score 100 = closed.",
            "_citation": "World Bank WDI: FS.AST.PRVT.GD.ZS, IT.NET.USER.ZS; WGI: RQ.EST",
        }
