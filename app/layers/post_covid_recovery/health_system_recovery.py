"""Health system recovery: health expenditure per capita recovery post-COVID.

COVID-19 disrupted routine healthcare delivery globally: elective procedures
cancelled, preventive care deferred, and non-COVID mortality rose. Health
expenditure per capita (WDI SH.XPD.CHEX.PC.CD) captures both public emergency
spending surges and the structural capacity of health systems.

Recovery is assessed relative to pre-pandemic trajectory. Sustained per capita
health spending at or above trend suggests system restoration; persistent drops
indicate under-investment that may produce long-run mortality and productivity costs.

Score: strong recovery (>=pre-pandemic level) -> STABLE, partial (5-15% below) -> WATCH,
significant gap (15-30% below) -> STRESS, severe gap (>30% below) -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class HealthSystemRecovery(LayerBase):
    layer_id = "lPC"
    name = "Health System Recovery"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "SH.XPD.CHEX.PC.CD"
        name = "health expenditure per capita"
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
                "error": "no data for SH.XPD.CHEX.PC.CD",
            }

        values = [(r["date"], r["value"]) for r in rows if r["value"] is not None]
        if len(values) < 2:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient observations"}

        values.sort(key=lambda x: x[0])

        pre_covid = [v for d, v in values if d < "2020-01-01"]
        latest = values[-1][1]

        if not pre_covid:
            pre_covid = [v for _, v in values[:-1]]

        baseline = pre_covid[-1] if pre_covid else latest

        # Trend growth rate from pre-covid period
        if len(pre_covid) >= 2:
            years = max(len(pre_covid) - 1, 1)
            trend_growth = (pre_covid[-1] / pre_covid[0]) ** (1 / years) - 1
            # Project forward by number of post-COVID years
            post_years = len([d for d, _ in values if d >= "2020-01-01"])
            projected = baseline * (1 + trend_growth) ** max(post_years, 1)
        else:
            projected = baseline

        # Gap between projected (trend) and actual
        if projected > 0:
            gap_pct = max(0.0, (projected - latest) / projected * 100)
        else:
            gap_pct = 0.0

        if gap_pct < 5:
            score = 5.0 + gap_pct * 1.0
        elif gap_pct < 15:
            score = 10.0 + (gap_pct - 5) * 3.0
        elif gap_pct < 30:
            score = 40.0 + (gap_pct - 15) * 2.0
        else:
            score = min(100.0, 70.0 + (gap_pct - 30) * 1.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "latest_health_exp_per_capita_usd": round(latest, 2),
                "pre_pandemic_baseline_usd": round(baseline, 2),
                "trend_projected_usd": round(projected, 2),
                "recovery_gap_pct": round(gap_pct, 2),
                "n_obs": len(values),
            },
        }
