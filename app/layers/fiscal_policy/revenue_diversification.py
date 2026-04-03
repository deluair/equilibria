"""Revenue Diversification module.

Measures the concentration of the tax base. Excessive dependence on trade
taxes (import/export duties) creates vulnerability to external shocks, while
a balanced mix (income, goods and services, trade) signals resilience.

Methodology:
- Query GC.TAX.YPKG.ZS (income, profits, capital gains tax, % of revenue).
- Query GC.TAX.IMPT.ZS (taxes on international trade, % of revenue).
- Query GC.TAX.TOTL.GD.ZS (total tax revenue, % GDP) as context.
- Herfindahl-Hirschman-style concentration: high trade-tax share = risk.
- trade_concentration = trade_pct / max(income_pct + trade_pct, 1e-10)
- Score = clip(trade_concentration * 80 + max(0, (trade_pct - 30)) * 0.8, 0, 100).

Sources: World Bank WDI (GC.TAX.YPKG.ZS, GC.TAX.IMPT.ZS, GC.TAX.TOTL.GD.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class RevenueDiversification(LayerBase):
    layer_id = "lFP"
    name = "Revenue Diversification"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        async def _latest(series_id: str) -> float | None:
            rows = await db.fetch_all(
                """
                SELECT dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                ORDER BY dp.date DESC
                LIMIT 1
                """,
                (country, series_id),
            )
            if rows:
                return float(rows[0]["value"])
            return None

        income_pct = await _latest("GC.TAX.YPKG.ZS")
        trade_pct = await _latest("GC.TAX.IMPT.ZS")
        total_tax = await _latest("GC.TAX.TOTL.GD.ZS")

        if income_pct is None and trade_pct is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        inc = income_pct or 0.0
        trd = trade_pct or 0.0

        # Trade concentration ratio
        denominator = inc + trd
        trade_concentration = trd / max(denominator, 1e-10)

        score = float(np.clip(
            trade_concentration * 80 + max(0.0, trd - 30) * 0.8,
            0,
            100,
        ))

        return {
            "score": round(score, 1),
            "country": country,
            "income_tax_pct_revenue": round(inc, 3),
            "trade_tax_pct_revenue": round(trd, 3),
            "total_tax_pct_gdp": round(total_tax, 3) if total_tax is not None else None,
            "trade_concentration_ratio": round(trade_concentration, 4),
            "high_trade_dependency": trd > 30,
            "indicators": ["GC.TAX.YPKG.ZS", "GC.TAX.IMPT.ZS", "GC.TAX.TOTL.GD.ZS"],
        }
