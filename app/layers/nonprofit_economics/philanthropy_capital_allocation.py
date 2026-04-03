"""Philanthropy capital allocation: charitable giving as share of GDP.

Proxied via personal remittances and private transfers received (WDI), which
capture cross-border private giving and solidarity transfers. In many economies,
formal philanthropy data is unavailable; remittances and private transfers are
the closest verifiable proxy for voluntary capital flows outside market channels.

Score: low giving capacity (<1% GDP) -> STABLE limited private giving,
moderate (1-3%) -> WATCH growing philanthropy culture, high (3-6%) ->
STRESS dependency on volatile flows, very high (>6%) -> CRISIS structural
dependence on remittance-type transfers.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class PhilanthropyCapitalAllocation(LayerBase):
    layer_id = "lNP"
    name = "Philanthropy Capital Allocation"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "BX.TRF.PWKR.DT.GD.ZS"
        name = "personal remittances received"
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
                "error": "no data for BX.TRF.PWKR.DT.GD.ZS",
            }

        values = [r["value"] for r in rows if r["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "all null values"}

        latest = values[0]
        trend = round(values[0] - values[-1], 3) if len(values) > 1 else None

        # Score: higher share of private transfers -> more developed giving culture
        # but also higher dependency risk
        if latest < 1.0:
            score = 10.0 + latest * 15.0
        elif latest < 3.0:
            score = 25.0 + (latest - 1.0) * 10.0
        elif latest < 6.0:
            score = 45.0 + (latest - 3.0) * 8.33
        else:
            score = min(100.0, 70.0 + (latest - 6.0) * 3.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "private_transfers_gdp_pct": round(latest, 2),
                "trend_pct_change": trend,
                "n_obs": len(values),
                "giving_regime": (
                    "limited" if latest < 1.0
                    else "growing" if latest < 3.0
                    else "significant" if latest < 6.0
                    else "high-dependency"
                ),
            },
        }
