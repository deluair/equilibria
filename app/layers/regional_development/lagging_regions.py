"""Lagging Regions module.

Proxies the agricultural-industrial income gap via the difference between
the share of employment in agriculture and the share of agriculture in GDP.
When a large fraction of the workforce is in agriculture but agriculture
contributes little to GDP, rural regions are structurally lagging behind
urban/industrial centres.

Score = clip((ag_empl_share - ag_gdp_share) * 2, 0, 100)

Sources: WDI NV.AGR.TOTL.ZS (agriculture % of GDP value added),
         WDI SL.AGR.EMPL.ZS (employment in agriculture % of total)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class LaggingRegions(LayerBase):
    layer_id = "lRD"
    name = "Lagging Regions"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows_gdp = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NV.AGR.TOTL.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        rows_empl = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SL.AGR.EMPL.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not rows_gdp or not rows_empl:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        gdp_vals = {r["date"]: float(r["value"]) for r in rows_gdp if r["value"] is not None}
        empl_vals = {r["date"]: float(r["value"]) for r in rows_empl if r["value"] is not None}

        common_dates = sorted(set(gdp_vals) & set(empl_vals), reverse=True)
        if not common_dates:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no overlapping dates"}

        gaps = []
        records = []
        for d in common_dates:
            ag_gdp = gdp_vals[d]
            ag_empl = empl_vals[d]
            gap = ag_empl - ag_gdp
            gaps.append(gap)
            records.append({"date": d, "ag_gdp_share": ag_gdp, "ag_empl_share": ag_empl, "gap": gap})

        mean_gap = float(np.mean(gaps))
        score = float(np.clip(mean_gap * 2, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "latest_date": common_dates[0],
            "latest_ag_gdp_share": round(records[0]["ag_gdp_share"], 2),
            "latest_ag_empl_share": round(records[0]["ag_empl_share"], 2),
            "latest_gap": round(records[0]["gap"], 2),
            "mean_gap": round(mean_gap, 2),
            "n_obs": len(gaps),
            "interpretation": "positive gap = more workers in ag than ag contributes to GDP (lagging rural regions)",
            "series": {"ag_gdp": "NV.AGR.TOTL.ZS", "ag_empl": "SL.AGR.EMPL.ZS"},
        }
