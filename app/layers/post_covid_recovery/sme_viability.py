"""SME viability: new business registration rate post-pandemic.

Small and medium enterprises (SMEs) account for ~90% of businesses and >50% of
employment globally (World Bank). COVID-19 caused mass SME closures: an estimated
100 million+ businesses were at high risk of permanent closure in 2020 (ILO).
Post-pandemic recovery depends critically on new firm formation replacing
destroyed firms.

WDI IC.BUS.NREG (new business registrations per 1,000 people aged 15-64) tracks
the entrepreneurial dynamism underpinning economic renewal.

Score: strong registration growth post-2020 -> STABLE,
flat or modest growth -> WATCH, decline from pre-pandemic -> STRESS,
sharp decline -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class SMEViability(LayerBase):
    layer_id = "lPC"
    name = "SME Viability"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "IC.BUS.NREG"
        name = "new business registrations"
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
                "error": "no data for IC.BUS.NREG",
            }

        values = [(r["date"], r["value"]) for r in rows if r["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "all null values"}

        values.sort(key=lambda x: x[0])
        latest = values[-1][1]

        pre_covid = [v for d, v in values if d < "2020-01-01"]
        baseline = pre_covid[-1] if pre_covid else values[0][1]

        if baseline <= 0:
            return {"score": None, "signal": "UNAVAILABLE", "error": "zero baseline for new business registrations"}

        change_pct = (latest - baseline) / baseline * 100

        # Score: positive change (growth) = lower stress; decline = higher stress
        if change_pct > 20:
            score = 5.0
        elif change_pct > 5:
            score = 5.0 + (20 - change_pct) * 1.33
        elif change_pct > -5:
            score = 25.0 + (5 - change_pct) * 2.5
        elif change_pct > -20:
            score = 50.0 + (-5 - change_pct) * 1.67
        else:
            score = min(100.0, 75.0 + (-20 - change_pct) * 1.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "latest_registrations_per_1000": round(latest, 3),
                "pre_pandemic_baseline": round(baseline, 3),
                "change_vs_pre_pandemic_pct": round(change_pct, 2),
                "recovery_above_baseline": latest >= baseline,
                "n_obs": len(values),
            },
        }
