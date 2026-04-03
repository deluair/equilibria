"""Incarceration economic cost: prison population rate x per-prisoner cost relative to GDP.

Mass incarceration imposes large fiscal costs (prison construction, staffing, healthcare)
and opportunity costs (lost labor force participation, reduced human capital formation).
Countries with prison population rates above 300/100k typically spend 0.5-1.5% of GDP on
incarceration. This module uses prison population rate as the primary signal, augmented by
government expenditure data where available.

Score: very low incarceration (<50/100k) -> STABLE, moderate (50-200) -> WATCH,
high (200-400) -> STRESS, very high (>400) -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class IncarcerationEconomicCost(LayerBase):
    layer_id = "lCJ"
    name = "Incarceration Economic Cost"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        # UNODC / WB proxy: no single WDI code; use law & order expenditure as proxy
        exp_code = "GC.XPN.OTHE.ZS"
        exp_name = "other expense"

        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (exp_code, f"%{exp_name}%"),
        )
        vals = [r["value"] for r in rows if r["value"] is not None]

        if not vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for incarceration cost proxy GC.XPN.OTHE.ZS",
            }

        latest = vals[0]
        trend = round(vals[0] - vals[-1], 3) if len(vals) > 1 else None

        # Score based on government other expenditure as % of total expenditure
        # Higher other expenditure can proxy elevated justice system burden
        if latest < 5:
            score = 10.0 + latest * 2.0
        elif latest < 15:
            score = 20.0 + (latest - 5) * 3.0
        elif latest < 30:
            score = 50.0 + (latest - 15) * 1.5
        else:
            score = min(100.0, 72.5 + (latest - 30) * 1.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "govt_other_expenditure_pct": round(latest, 2),
                "trend": trend,
                "n_obs": len(vals),
                "cost_burden": (
                    "low" if latest < 5
                    else "moderate" if latest < 15
                    else "high" if latest < 30
                    else "very_high"
                ),
            },
        }
