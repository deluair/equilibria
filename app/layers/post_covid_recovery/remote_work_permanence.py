"""Remote work permanence: ICT service exports growth as remote work proxy.

The pandemic permanently altered labor geography. Remote-capable occupations
shifted en masse to hybrid/remote arrangements. ICT service exports (WDI
BX.GSR.CCIS.ZS: computer, communications and other services as % of service
exports) serve as a structural proxy for the embedded remote-work economy.

Sustained growth in ICT service exports post-2020 indicates economies have
successfully integrated remote work as a permanent feature. Stagnation or
decline may indicate digital infrastructure constraints or skills mismatches
preventing the shift.

Score: strong ICT growth (>15% cumulative since 2019) -> STABLE,
moderate growth (5-15%) -> WATCH, flat or small (0-5%) -> STRESS,
decline -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class RemoteWorkPermanence(LayerBase):
    layer_id = "lPC"
    name = "Remote Work Permanence"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "BX.GSR.CCIS.ZS"
        name = "ICT service exports"
        rows = await db.fetch_all(
            "SELECT value, date FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for BX.GSR.CCIS.ZS",
            }

        values = [(r["date"], r["value"]) for r in rows if r["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "all null values"}

        values.sort(key=lambda x: x[0])
        latest = values[-1][1]

        # Find pre-COVID ICT export share
        pre_covid = [v for d, v in values if d < "2020-01-01"]
        baseline = pre_covid[-1] if pre_covid else values[0][1]

        if baseline > 0:
            growth_pct = (latest - baseline) / baseline * 100
        else:
            growth_pct = 0.0

        # Score inverted: more ICT growth = better remote work adoption = lower stress
        if growth_pct > 15:
            score = 5.0
        elif growth_pct > 5:
            score = 5.0 + (15 - growth_pct) * 2.0
        elif growth_pct > 0:
            score = 25.0 + (5 - growth_pct) * 5.0
        elif growth_pct > -10:
            score = 50.0 + abs(growth_pct) * 2.0
        else:
            score = min(100.0, 70.0 + (abs(growth_pct) - 10) * 1.0)

        trend = round(values[-1][1] - values[0][1], 3) if len(values) > 1 else None

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "latest_ict_exports_pct_services": round(latest, 2),
                "pre_pandemic_baseline_pct": round(baseline, 2),
                "ict_growth_since_2019_pct": round(growth_pct, 2),
                "overall_trend": trend,
                "n_obs": len(values),
            },
        }
