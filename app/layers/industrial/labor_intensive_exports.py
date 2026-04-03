"""Labor-intensive exports and manufacturing employment productivity.

Labor-intensive manufactured exports (garments, footwear, furniture, toys)
are a critical early-stage development pathway for low-wage economies. They
absorb rural-to-urban migrants, generate foreign exchange, and initiate
learning-by-exporting productivity gains (Hsieh & Klenow 2009).

Manufacturing export share (TX.VAL.MANF.ZS.UN) measures manufactured goods
as a proportion of total merchandise exports. High share = manufacturing
export orientation.

Wage employment share (SL.EMP.WORK.ZS) measures employees in formal
wage-paying jobs as a % of total employment. High manufacturing exports
relative to formal wage employment indicates labor productivity in export
manufacturing -- a positive development signal.

Productivity proxy:
    If both indicators available: ratio = manf_export_share / wage_emp_share
    High ratio (> 1.5) = manufacturing punches above its wage-employment weight
    (high labor productivity or capital-intensive manufactures)
    Low ratio (< 0.5) = labor-intensive manufacturing with high employment but
    modest export share (volume play, lower quality)

Score construction:
    Base score from manufacturing export share: score_base = max(0, 50 - manf_pct)
    Productivity adjustment: if ratio > 1.5, reduce score by 10 (good)
                             if ratio < 0.5, increase score by 15 (stress)
    score = clip(score_base + adjustment, 0, 100)

References:
    Hsieh, C. & Klenow, P. (2009). Misallocation and manufacturing TFP in China
        and India. QJE 124(4): 1403-1448.
    Kucera, D. & Roncolato, L. (2008). Informal employment: Two contested policy
        issues. International Labour Review 147(4): 321-348.
    World Bank WDI: TX.VAL.MANF.ZS.UN, SL.EMP.WORK.ZS.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class LaborIntensiveExports(LayerBase):
    layer_id = "l14"
    name = "Labor Intensive Exports"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        manf_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.series_id = 'TX.VAL.MANF.ZS.UN'
              AND ds.country_iso3 = ?
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        wage_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.series_id = 'SL.EMP.WORK.ZS'
              AND ds.country_iso3 = ?
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not manf_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no manufacturing export share data",
            }

        manf_pct = float(manf_rows[0]["value"])
        manf_year = manf_rows[0]["date"]

        wage_pct = float(wage_rows[0]["value"]) if wage_rows else None
        wage_year = wage_rows[0]["date"] if wage_rows else None

        ratio = None
        adjustment = 0.0
        if wage_pct is not None and wage_pct > 0:
            ratio = manf_pct / wage_pct
            if ratio > 1.5:
                adjustment = -10.0
            elif ratio < 0.5:
                adjustment = 15.0

        score_base = max(0.0, 50.0 - manf_pct)
        score = float(np.clip(score_base + adjustment, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "country": country,
            "manufacturing_export_share_pct": round(manf_pct, 2),
            "manf_year": manf_year,
            "wage_employment_share_pct": round(wage_pct, 2) if wage_pct is not None else None,
            "wage_year": wage_year,
            "productivity_ratio": round(ratio, 3) if ratio is not None else None,
            "productivity_signal": (
                "high productivity" if (ratio or 0) > 1.5
                else "labor intensive" if (ratio or 0) < 0.5
                else "balanced"
            ) if ratio is not None else None,
            "export_orientation": (
                "manufacturing-dominant" if manf_pct >= 60
                else "manufacturing-significant" if manf_pct >= 30
                else "commodity-leaning"
            ),
        }
