"""Power grid reliability: outage frequency and duration.

Frequent and prolonged power outages impose direct costs on firms and households,
signal fragile grid infrastructure, and undermine industrial competitiveness.
WDI indicators IC.ELC.OUTG (number of outages experienced by firms per month)
and IC.ELC.TIME (time to obtain electricity connection in days) are used as
dual measures of reliability and institutional responsiveness.

Score: low outage frequency and fast connection -> STABLE, high outages and
slow connections -> CRISIS unreliable grid damaging productive capacity.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class PowerGridReliability(LayerBase):
    layer_id = "lES"
    name = "Power Grid Reliability"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        outg_code = "IC.ELC.OUTG"
        time_code = "IC.ELC.TIME"

        outg_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (outg_code, "%outages%"),
        )
        time_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (time_code, "%electricity%days%"),
        )

        outg_vals = [r["value"] for r in outg_rows if r["value"] is not None]
        time_vals = [r["value"] for r in time_rows if r["value"] is not None]

        if not outg_vals and not time_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for grid reliability (IC.ELC.OUTG, IC.ELC.TIME)",
            }

        outages = outg_vals[0] if outg_vals else None
        conn_days = time_vals[0] if time_vals else None

        # Score each dimension separately, average available ones
        component_scores = []

        if outages is not None:
            # Outages per month: 0 -> 5, 1-5 -> WATCH, 5-15 -> STRESS, >15 -> CRISIS
            if outages < 1:
                s_outg = 5.0 + outages * 15.0
            elif outages < 5:
                score_base = 20.0 + (outages - 1) * 5.0
                s_outg = score_base
            elif outages < 15:
                s_outg = 40.0 + (outages - 5) * 2.5
            else:
                s_outg = min(100.0, 65.0 + (outages - 15) * 1.0)
            component_scores.append(round(s_outg, 2))

        if conn_days is not None:
            # Days to get electricity connection: <30 -> STABLE, 30-90 -> WATCH,
            # 90-180 -> STRESS, >180 -> CRISIS
            if conn_days < 30:
                s_time = 5.0 + conn_days * 0.5
            elif conn_days < 90:
                s_time = 20.0 + (conn_days - 30) * 0.5
            elif conn_days < 180:
                s_time = 50.0 + (conn_days - 90) * 0.28
            else:
                s_time = min(100.0, 75.0 + (conn_days - 180) * 0.14)
            component_scores.append(round(s_time, 2))

        score = round(sum(component_scores) / len(component_scores), 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "outages_per_month": round(outages, 2) if outages is not None else None,
                "days_to_electricity_connection": round(conn_days, 1) if conn_days is not None else None,
                "n_obs_outages": len(outg_vals),
                "n_obs_connection_time": len(time_vals),
                "component_scores": component_scores,
                "reliability_tier": (
                    "reliable" if score < 25
                    else "moderate" if score < 50
                    else "unreliable" if score < 75
                    else "crisis"
                ),
            },
        }
