"""SME Credit Access module.

Measures SME access to formal credit relative to their economic contribution.
Uses World Bank WDI:
- FP.CRE.DOMN.ZS: Domestic credit to private sector (% GDP) -- credit depth proxy
- FB.AST.NPER.ZS: Bank nonperforming loans to total gross loans (%) -- credit quality

A deep private credit market with low NPLs indicates better credit access for SMEs.
Constrained credit markets and high NPLs suggest SMEs face financing barriers,
limiting their growth and contribution to employment and output.

Score: higher score = more constrained SME credit environment = more stress.

Sources: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class SmeCreditAccess(LayerBase):
    layer_id = "lER"
    name = "SME Credit Access"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        credit_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FP.CRE.DOMN.ZS'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        npl_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FB.AST.NPER.ZS'
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        credit_depth: float | None = None
        npl_rate: float | None = None

        if credit_rows:
            vals = [float(r["value"]) for r in credit_rows if r["value"] is not None]
            credit_depth = float(np.mean(vals)) if vals else None

        if npl_rows:
            vals = [float(r["value"]) for r in npl_rows if r["value"] is not None]
            npl_rate = float(np.mean(vals)) if vals else None

        if credit_depth is None and npl_rate is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no credit data available"}

        score_parts: list[float] = []

        if credit_depth is not None:
            # Credit depth: 0-200% GDP. Higher = better access. Clamp at 150%.
            norm = min(100.0, (credit_depth / 150.0) * 100.0)
            score_parts.append(max(0.0, 100.0 - norm))

        if npl_rate is not None:
            # NPL rate: 0-30%+. Higher = worse credit quality / access.
            npl_score = min(100.0, (npl_rate / 20.0) * 100.0)
            score_parts.append(npl_score)

        score = float(np.mean(score_parts))

        return {
            "score": round(score, 1),
            "country": country,
            "private_credit_pct_gdp": round(credit_depth, 2) if credit_depth is not None else None,
            "npl_rate_pct": round(npl_rate, 2) if npl_rate is not None else None,
            "interpretation": "High score = shallow credit + high NPLs = SME financing barriers",
        }
