"""Search friction index: unemployment-to-vacancy ratio as Beveridge curve tightness.

The Beveridge curve plots unemployment against job vacancies. A high
unemployment-to-vacancy ratio signals low labor market tightness: many
unemployed workers competing for few vacancies, implying significant search
friction. A tight market (low ratio) indicates efficient matching with
workers quickly absorbed into available roles.

Score: low ratio (<0.5) -> STABLE tight market, moderate (0.5-2.0) ->
WATCH normal friction, high (2.0-5.0) -> STRESS elevated search friction,
very high (>5.0) -> CRISIS severe mismatch with structural unemployment.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class SearchFrictionIndex(LayerBase):
    layer_id = "lLM"
    name = "Search Friction Index"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        unemp_code = "SL.UEM.TOTL.ZS"
        vac_code = "SL.UEM.NEET.ZS"

        unemp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (unemp_code, "%unemployment%total%"),
        )
        vac_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (vac_code, "%NEET%"),
        )

        unemp_vals = [r["value"] for r in unemp_rows if r["value"] is not None]

        if not unemp_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for unemployment rate SL.UEM.TOTL.ZS",
            }

        latest_unemp = unemp_vals[0]
        vac_vals = [r["value"] for r in vac_rows if r["value"] is not None]

        # If vacancy data available, compute ratio; otherwise proxy from unemployment level
        if vac_vals:
            vacancy_proxy = vac_vals[0]
            ratio = latest_unemp / vacancy_proxy if vacancy_proxy > 0 else latest_unemp * 2.0
        else:
            # Proxy: high unemployment implies high search friction without vacancy offset
            ratio = latest_unemp / 5.0  # normalize by typical 5% frictional baseline

        trend = round(unemp_vals[0] - unemp_vals[-1], 3) if len(unemp_vals) > 1 else None

        if ratio < 0.5:
            score = ratio * 30.0
        elif ratio < 2.0:
            score = 15.0 + (ratio - 0.5) * 15.0
        elif ratio < 5.0:
            score = 37.5 + (ratio - 2.0) * 10.0
        else:
            score = min(100.0, 67.5 + (ratio - 5.0) * 3.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "unemployment_rate_pct": round(latest_unemp, 2),
                "uv_ratio_proxy": round(ratio, 3),
                "unemployment_trend": trend,
                "n_obs_unemp": len(unemp_vals),
                "vacancy_data_available": bool(vac_vals),
            },
        }
