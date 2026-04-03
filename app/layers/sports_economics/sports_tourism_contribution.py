"""Sports tourism contribution: sports-driven international tourist arrivals.

International inbound tourist arrivals (WDI ST.INT.ARVL) directly measure
the flow of visitors whose primary or secondary motivation includes attending
or participating in sporting events. Absolute arrival volumes reflect a
country's attractiveness as a sports tourism destination; year-on-year growth
captures event-cycle dynamics (Olympic cycle, World Cup quadrennial pattern).

Score: based on arrivals level (millions) and growth trajectory.
Low arrivals (<5M) -> STABLE niche market; moderate (5-20M) -> WATCH
growing destination; high (20-50M) -> STRESS capacity and sustainability
pressure; very high (>50M) -> CRISIS overcrowding and infrastructure strain.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class SportsTourismContribution(LayerBase):
    layer_id = "lSP"
    name = "Sports Tourism Contribution"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "ST.INT.ARVL"
        name = "tourist arrivals"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        values = [r["value"] for r in rows if r["value"] is not None]
        if not values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for ST.INT.ARVL",
            }

        latest = values[0]
        arrivals_m = latest / 1_000_000.0
        trend = round((values[0] - values[-1]) / max(values[-1], 1) * 100, 2) if len(values) > 1 else None

        if arrivals_m < 5.0:
            score = 5.0 + arrivals_m * 3.0
        elif arrivals_m < 20.0:
            score = 20.0 + (arrivals_m - 5.0) * 2.0
        elif arrivals_m < 50.0:
            score = 50.0 + (arrivals_m - 20.0) * 0.83
        else:
            score = min(100.0, 75.0 + (arrivals_m - 50.0) * 0.5)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "inbound_arrivals_millions": round(arrivals_m, 2),
                "arrivals_raw": int(latest),
                "cumulative_growth_pct": trend,
                "n_obs": len(values),
                "destination_tier": (
                    "niche" if arrivals_m < 5.0
                    else "growing" if arrivals_m < 20.0
                    else "major" if arrivals_m < 50.0
                    else "mass"
                ),
            },
        }
