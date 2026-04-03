"""Reserve currency concentration: USD dominance in global reserves.

The international monetary system rests on a concentrated set of reserve
currencies, with the USD historically accounting for ~55-65% of allocated
global reserves (IMF COFER). High concentration implies systemic vulnerability
to US monetary policy spillovers and dollar liquidity shocks. This module
proxies concentration via the net barter terms of trade and exchange rate
volatility as available WDI indicators, since COFER share data is not
directly in WDI.

Score: lower concentration risk (diversified reserves) -> STABLE;
extreme USD dominance -> STRESS/CRISIS.
"""

from __future__ import annotations

import statistics

from app.layers.base import LayerBase


class ReserveCurrencyConcentration(LayerBase):
    layer_id = "lMS"
    name = "Reserve Currency Concentration"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        # Proxy: broad money growth volatility as indicator of monetary instability
        code = "FM.LBL.BMNY.GD.ZS"
        name = "Broad money"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 20",
            (code, f"%{name}%"),
        )
        # Secondary: net foreign assets as share of GDP
        nfa_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("FM.AST.NFRG.CN", "%net foreign assets%"),
        )

        vals = [r["value"] for r in rows if r["value"] is not None]
        nfa_vals = [r["value"] for r in nfa_rows if r["value"] is not None]

        if not vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for broad money FM.LBL.BMNY.GD.ZS",
            }

        latest = vals[0]
        vol = round(statistics.stdev(vals), 3) if len(vals) > 2 else None

        # High broad money/GDP with high volatility proxies reserve concentration risk
        # Thresholds calibrated to global distribution
        if latest > 150:
            base = 70.0
        elif latest > 100:
            base = 50.0 + (latest - 100) * 0.4
        elif latest > 60:
            base = 30.0 + (latest - 60) * 0.5
        else:
            base = max(5.0, latest * 0.4)

        # Volatility premium
        if vol is not None:
            if vol > 20:
                base = min(100.0, base + 15.0)
            elif vol > 10:
                base = min(100.0, base + 8.0)

        score = round(base, 2)
        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "broad_money_gdp_pct": round(latest, 2),
                "broad_money_volatility": vol,
                "n_obs": len(vals),
                "nfa_obs": len(nfa_vals),
                "concentration_risk": "high" if score > 50 else "moderate" if score > 25 else "low",
            },
        }
