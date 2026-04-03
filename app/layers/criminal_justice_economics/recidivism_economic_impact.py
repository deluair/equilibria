"""Recidivism economic impact: reoffending rate impact on labor market reintegration.

High recidivism prevents formerly incarcerated individuals from rejoining the productive
labor force, creating persistent welfare dependency and lost human capital. Recidivism
rates of 60-70% within 3 years are common in high-incarceration countries. Economic
cost includes foregone earnings, continued incarceration costs, and social welfare burden.
Proxied by youth unemployment and NEET rates, which correlate strongly with first-time
and repeat offending cycles.

Score: low NEET and youth unemployment (strong reintegration pathways) -> STABLE,
moderate -> WATCH, high exclusion rates -> STRESS, severe labor exclusion -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class RecidivismEconomicImpact(LayerBase):
    layer_id = "lCJ"
    name = "Recidivism Economic Impact"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        # Youth not in education, employment or training (NEET) % as reintegration proxy
        neet_code = "SL.UEM.NEET.ZS"
        neet_name = "NEET"

        yu_code = "SL.UEM.1524.ZS"
        yu_name = "youth unemployment"

        neet_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (neet_code, f"%{neet_name}%"),
        )
        yu_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (yu_code, f"%{yu_name}%"),
        )

        neet_vals = [r["value"] for r in neet_rows if r["value"] is not None]
        yu_vals = [r["value"] for r in yu_rows if r["value"] is not None]

        if not neet_vals and not yu_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for recidivism proxy (NEET/youth unemployment)",
            }

        # Prefer NEET; fall back to youth unemployment
        if neet_vals:
            primary = neet_vals[0]
            primary_label = "neet_pct"
            n_obs = len(neet_vals)
            trend = round(neet_vals[0] - neet_vals[-1], 3) if len(neet_vals) > 1 else None
        else:
            primary = yu_vals[0]
            primary_label = "youth_unemployment_pct"
            n_obs = len(yu_vals)
            trend = round(yu_vals[0] - yu_vals[-1], 3) if len(yu_vals) > 1 else None

        if primary < 5:
            score = 5.0 + primary * 2.0
        elif primary < 15:
            score = 15.0 + (primary - 5) * 3.5
        elif primary < 30:
            score = 50.0 + (primary - 15) * 2.0
        else:
            score = min(100.0, 80.0 + (primary - 30) * 1.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                primary_label: round(primary, 2),
                "trend": trend,
                "n_obs": n_obs,
                "reintegration_risk": (
                    "low" if primary < 5
                    else "moderate" if primary < 15
                    else "high" if primary < 30
                    else "very_high"
                ),
            },
        }
