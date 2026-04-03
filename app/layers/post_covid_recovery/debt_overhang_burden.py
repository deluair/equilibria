"""Debt overhang burden: government debt/GDP increase from 2019 to latest year.

COVID-19 triggered the largest peacetime fiscal expansion in history. Countries
spent heavily on emergency transfers, health response, and economic stabilization,
driving median advanced economy debt/GDP above 120% by 2021. High debt overhangs
constrain future fiscal space, raise sovereign risk premia, and may crowd out
private investment (Reinhart & Rogoff 2010; Cecchetti et al. 2011 threshold ~85%).

WDI indicator: GC.DOD.TOTL.GD.ZS (central government debt, total % of GDP).

Score: small increase (<10pp) -> STABLE, moderate (10-25pp) -> WATCH,
large (25-45pp) -> STRESS, very large (>45pp) -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class DebtOverhangBurden(LayerBase):
    layer_id = "lPC"
    name = "Debt Overhang Burden"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "GC.DOD.TOTL.GD.ZS"
        name = "central government debt"
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
                "error": "no data for GC.DOD.TOTL.GD.ZS",
            }

        values = [(r["date"], r["value"]) for r in rows if r["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "all null values"}

        values.sort(key=lambda x: x[0])
        latest_val = values[-1][1]

        # Find 2019 baseline (pre-COVID)
        pre_covid = [v for d, v in values if d < "2020-01-01"]
        baseline = pre_covid[-1] if pre_covid else values[0][1]

        debt_increase = latest_val - baseline

        # Score: higher increase = greater overhang burden
        if debt_increase < 0:
            score = 5.0  # Debt reduction, low stress
        elif debt_increase < 10:
            score = 5.0 + debt_increase * 1.5
        elif debt_increase < 25:
            score = 20.0 + (debt_increase - 10) * 2.0
        elif debt_increase < 45:
            score = 50.0 + (debt_increase - 25) * 1.5
        else:
            score = min(100.0, 80.0 + (debt_increase - 45) * 0.5)

        # Absolute level adjustment: debt above 85% of GDP adds structural risk
        if latest_val > 100:
            score = min(100.0, score + 8.0)
        elif latest_val > 85:
            score = min(100.0, score + 4.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "latest_debt_gdp_pct": round(latest_val, 2),
                "pre_covid_baseline_pct": round(baseline, 2),
                "debt_increase_pp": round(debt_increase, 2),
                "above_reinhart_rogoff_threshold": latest_val > 85,
                "n_obs": len(values),
            },
        }
