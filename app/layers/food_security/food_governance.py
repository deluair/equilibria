"""Food system governance: regulatory quality and agricultural policy trajectory.

Effective food governance requires both capable state institutions (regulatory
quality) and a productive agricultural sector that state policy supports.
Weak regulatory quality undermines food safety, market functioning, and price
stabilization. A declining agricultural value-added trend signals governance
failure or policy neglect of the food sector.

Methodology:
    reg_quality : RQ.EST (World Governance Indicators Regulatory Quality,
                  scale -2.5 to +2.5, higher = better)
    ag_share_ts : NV.AGR.TOTL.ZS time series (agriculture % of GDP)
                  used to compute trend direction via linregress

    reg_quality_stress = clip((2.5 - RQ.EST) / 5.0 * 100, 0, 100)
        (-2.5 worst = score 100; +2.5 best = score 0)

    ag_trend_stress: slope of agriculture share over time.
        declining ag share (negative slope) = governance stress.
        stress = clip(-slope * 20, 0, 50)
        (slope = -2.5 pct pts/year -> stress = 50)

    score = clip(0.6 * reg_quality_stress + 0.4 * ag_trend_stress, 0, 100)

Score (0-100): Higher score = greater food governance weakness.

References:
    Kaufmann, D., Kraay, A. & Mastruzzi, M. (2010). "The Worldwide
        Governance Indicators." World Bank Policy Research Working Paper 5430.
    World Bank (2023). WDI: NV.AGR.TOTL.ZS.
    World Bank WGI: RQ.EST.
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class FoodGovernance(LayerBase):
    layer_id = "lFS"
    name = "Food Governance"

    async def compute(self, db, **kwargs) -> dict:
        """Compute food system governance stress score.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
            min_obs : int - minimum ag trend observations (default 5)
        """
        country = kwargs.get("country_iso3", "BGD")
        min_obs = int(kwargs.get("min_obs", 5))

        reg_row = await db.fetch_one(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.indicator_code = 'RQ.EST'
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )
        if not reg_row:
            reg_row = await db.fetch_one(
                """
                SELECT dp.value, dp.date
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.name LIKE '%regulatory%quality%'
                ORDER BY dp.date DESC
                LIMIT 1
                """,
                (country,),
            )

        ag_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.indicator_code = 'NV.AGR.TOTL.ZS'
            ORDER BY dp.date ASC
            """,
            (country,),
        )
        if not ag_rows or len(ag_rows) < min_obs:
            ag_rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.name LIKE '%agriculture%value%added%%GDP%'
                ORDER BY dp.date ASC
                """,
                (country,),
            )

        if not reg_row and (not ag_rows or len(ag_rows) < min_obs):
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no regulatory quality or agricultural trend data available",
            }

        reg_quality = float(reg_row["value"]) if reg_row and reg_row["value"] is not None else None

        reg_quality_stress = (
            float(np.clip((2.5 - reg_quality) / 5.0 * 100.0, 0, 100))
            if reg_quality is not None
            else 50.0
        )

        ag_trend_stress = 0.0
        ag_trend_detail: dict = {}
        if ag_rows and len(ag_rows) >= min_obs:
            years, vals = [], []
            for r in ag_rows:
                if r["value"] is not None:
                    try:
                        years.append(int(str(r["date"])[:4]))
                        vals.append(float(r["value"]))
                    except (ValueError, TypeError):
                        continue

            if len(years) >= min_obs:
                t = np.array(years, dtype=float)
                y = np.array(vals, dtype=float)
                res = linregress(t, y)
                slope = float(res.slope)
                ag_trend_stress = float(np.clip(-slope * 20.0, 0, 50))
                ag_trend_detail = {
                    "slope_per_year": round(slope, 4),
                    "r_squared": round(float(res.rvalue ** 2), 4),
                    "p_value": round(float(res.pvalue), 4),
                    "direction": "declining" if slope < 0 else "growing",
                    "n_obs": len(years),
                    "period": {"start": int(t[0]), "end": int(t[-1])},
                }

        score = float(np.clip(0.6 * reg_quality_stress + 0.4 * ag_trend_stress, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "regulatory_quality_wgi": round(reg_quality, 4) if reg_quality is not None else None,
            "component_scores": {
                "reg_quality_stress": round(reg_quality_stress, 2),
                "ag_trend_stress": round(ag_trend_stress, 2),
            },
            "agricultural_trend": ag_trend_detail,
            "reg_quality_date": reg_row["date"] if reg_row else None,
            "indicators": ["RQ.EST", "NV.AGR.TOTL.ZS"],
        }
