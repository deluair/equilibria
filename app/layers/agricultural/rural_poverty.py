"""Rural poverty proxy: agricultural value added per worker gap.

Estimates rural poverty risk by comparing the agricultural sector's share of
GDP to its share of employment. A large gap between these two shares indicates
that a disproportionately large share of the labor force is concentrated in
a low-productivity sector, which is the classic structural signature of a
rural poverty trap (Lewis dual-sector model).

Methodology:
    Query:
        - NV.AGR.TOTL.ZS: Agriculture value added as % of GDP (ag GDP share)
        - SL.AGR.EMPL.ZS: Employment in agriculture as % of total employment

    Poverty gap ratio = ag_employment_share / ag_gdp_share

    When employment share >> GDP share, agricultural workers earn far below
    average income. The score captures this structural imbalance:

        score = clip((gap_ratio - 1) * 25, 0, 100)

    gap_ratio = 1: parity (no structural gap), score = 0.
    gap_ratio = 5: severe gap (5x as many workers as economic output share),
    score = 100.

Score (0-100): Higher score indicates greater rural poverty trap risk.

References:
    Lewis, W.A. (1954). "Economic Development with Unlimited Supplies of Labour."
        The Manchester School, 22(2), 139-191.
    World Bank WDI indicators NV.AGR.TOTL.ZS and SL.AGR.EMPL.ZS.
    Timmer, C.P. (1988). "The agricultural transformation." Handbook of
        Development Economics.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class RuralPoverty(LayerBase):
    layer_id = "l5"
    name = "Rural Poverty Trap"

    async def compute(self, db, **kwargs) -> dict:
        """Compute rural poverty proxy score.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
        """
        country = kwargs.get("country_iso3", "BGD")

        # Agricultural value added as % of GDP
        row_gdp = await db.fetch_one(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.indicator_code = 'NV.AGR.TOTL.ZS'
                   OR ds.name LIKE '%agriculture%value%added%GDP%')
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )

        # Agricultural employment share
        row_empl = await db.fetch_one(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.indicator_code = 'SL.AGR.EMPL.ZS'
                   OR ds.name LIKE '%employment%agriculture%')
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )

        if not row_gdp or row_gdp["value"] is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "agricultural GDP share data unavailable (NV.AGR.TOTL.ZS)",
            }

        if not row_empl or row_empl["value"] is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "agricultural employment share data unavailable (SL.AGR.EMPL.ZS)",
            }

        ag_gdp_share = float(row_gdp["value"])
        ag_empl_share = float(row_empl["value"])

        if ag_gdp_share <= 0:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "agricultural GDP share is zero or negative, cannot compute gap",
            }

        gap_ratio = ag_empl_share / ag_gdp_share

        # score = clip((gap_ratio - 1) * 25, 0, 100)
        score = float(np.clip((gap_ratio - 1.0) * 25.0, 0.0, 100.0))

        trap_severity = (
            "severe" if gap_ratio > 4
            else "high" if gap_ratio > 2.5
            else "moderate" if gap_ratio > 1.5
            else "low"
        )

        return {
            "score": round(score, 2),
            "country": country,
            "ag_gdp_share_pct": round(ag_gdp_share, 2),
            "ag_employment_share_pct": round(ag_empl_share, 2),
            "gap_ratio": round(gap_ratio, 4),
            "trap_severity": trap_severity,
            "ag_gdp_date": row_gdp["date"],
            "ag_empl_date": row_empl["date"],
            "indicators": {
                "gdp_share": "NV.AGR.TOTL.ZS",
                "employment_share": "SL.AGR.EMPL.ZS",
            },
            "interpretation": (
                "gap_ratio > 1 means agricultural employment share exceeds "
                "agricultural GDP share, indicating lower productivity and "
                "incomes for rural workers relative to the national average"
            ),
        }
