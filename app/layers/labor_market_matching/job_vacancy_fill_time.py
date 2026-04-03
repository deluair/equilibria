"""Job vacancy fill time: time-to-fill proxy via unemployment duration and vacancy rate.

Longer unemployment duration combined with a high vacancy rate indicates
structural matching failures -- employers struggle to fill positions despite
available workers. This module proxies time-to-fill using mean unemployment
duration (weeks) adjusted by the prevailing vacancy rate.

Score: short duration / low vacancies -> STABLE efficient market, moderate
duration -> WATCH emerging friction, long duration (>26 weeks) -> STRESS
structural barriers to matching, extreme duration -> CRISIS dysfunctional market.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class JobVacancyFillTime(LayerBase):
    layer_id = "lLM"
    name = "Job Vacancy Fill Time"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        duration_code = "SL.UEM.DURS.ZS"
        unemp_code = "SL.UEM.TOTL.ZS"

        duration_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (duration_code, "%long-term unemployment%"),
        )
        unemp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (unemp_code, "%unemployment%total%"),
        )

        duration_vals = [r["value"] for r in duration_rows if r["value"] is not None]
        unemp_vals = [r["value"] for r in unemp_rows if r["value"] is not None]

        if not duration_vals and not unemp_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for unemployment duration or rate",
            }

        if duration_vals:
            # Long-term unemployment share (% of total unemployed) as fill-time proxy
            lt_unemp_share = duration_vals[0]
            # Scale: >50% long-term share -> very long fill times
            if lt_unemp_share < 15:
                score = lt_unemp_share * 1.0
            elif lt_unemp_share < 30:
                score = 15.0 + (lt_unemp_share - 15) * 2.0
            elif lt_unemp_share < 50:
                score = 45.0 + (lt_unemp_share - 30) * 1.5
            else:
                score = min(100.0, 75.0 + (lt_unemp_share - 50) * 0.5)
            duration_trend = (
                round(duration_vals[0] - duration_vals[-1], 3) if len(duration_vals) > 1 else None
            )
        else:
            # Fallback: use unemployment rate as fill-time proxy
            lt_unemp_share = None
            latest_unemp = unemp_vals[0]
            if latest_unemp < 5:
                score = latest_unemp * 3.0
            elif latest_unemp < 10:
                score = 15.0 + (latest_unemp - 5) * 4.0
            elif latest_unemp < 20:
                score = 35.0 + (latest_unemp - 10) * 3.0
            else:
                score = min(100.0, 65.0 + (latest_unemp - 20) * 1.5)
            duration_trend = None

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "long_term_unemp_share_pct": round(lt_unemp_share, 2) if lt_unemp_share is not None else None,
                "unemployment_rate_pct": round(unemp_vals[0], 2) if unemp_vals else None,
                "duration_trend": duration_trend,
                "n_obs_duration": len(duration_vals),
                "n_obs_unemp": len(unemp_vals),
                "fill_time_signal": "long" if score > 50 else "normal",
            },
        }
