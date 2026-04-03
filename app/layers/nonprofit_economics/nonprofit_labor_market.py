"""Nonprofit labor market: wage differential between nonprofit and for-profit.

Nonprofits typically pay below market wages, relying on mission alignment and
intrinsic motivation. The wage differential constrains talent attraction. Proxied
via public sector wage bill as share of GDP (WDI: GB.XPD.COMP.ZS) -- public
sector wages set floor compensation for nonprofit employment markets, especially
in developing economies where public sector dominates formal employment.

Score: very high wage bill -> STRESS fiscal crowding out of nonprofit compensation;
very low -> CRISIS insufficient wage floor for sustainable nonprofit labor.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class NonprofitLaborMarket(LayerBase):
    layer_id = "lNP"
    name = "Nonprofit Labor Market"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "GB.XPD.COMP.ZS"
        name = "compensation of employees"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for GB.XPD.COMP.ZS (public wage bill)",
            }

        values = [r["value"] for r in rows if r["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "all null values"}

        latest = values[0]
        trend = round(values[0] - values[-1], 3) if len(values) > 1 else None

        # Wage bill % of GDP: 5-10% typical healthy range for nonprofits to compete
        # <5% -> CRISIS (too low, no wage floor), >15% -> STRESS (crowding out)
        if latest < 5.0:
            score = 70.0 + (5.0 - latest) * 3.0
        elif latest < 8.0:
            score = 15.0 + (latest - 5.0) * 5.0
        elif latest < 12.0:
            score = 30.0 + (latest - 8.0) * 5.0
        elif latest < 15.0:
            score = 50.0 + (latest - 12.0) * 5.0
        else:
            score = min(100.0, 65.0 + (latest - 15.0) * 2.5)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "public_wage_bill_gdp_pct": round(latest, 2),
                "trend_pct_change": trend,
                "n_obs": len(values),
                "labor_market_regime": (
                    "insufficient_floor" if latest < 5.0
                    else "competitive" if latest < 8.0
                    else "moderate" if latest < 12.0
                    else "pressured" if latest < 15.0
                    else "crowding_out"
                ),
            },
        }
