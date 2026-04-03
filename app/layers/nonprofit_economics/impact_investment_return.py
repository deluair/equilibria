"""Impact investment return: ESG capital market development relative to total markets.

Impact investing channels capital toward measurable social and environmental
outcomes alongside financial return. Proxied via stock market capitalization
as % of GDP (CM.MKT.LCAP.GD.ZS) and domestic credit to private sector
(FS.AST.PRVT.GD.ZS) as indicators of capital market depth enabling ESG flows.
Deeper markets = greater capacity to absorb and allocate impact capital.

Score: shallow markets (low cap + low credit) -> CRISIS constrained impact
investing; deep markets -> STABLE enabling ESG allocation.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class ImpactInvestmentReturn(LayerBase):
    layer_id = "lNP"
    name = "Impact Investment Return"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        mkt_code = "CM.MKT.LCAP.GD.ZS"
        credit_code = "FS.AST.PRVT.GD.ZS"

        mkt_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (mkt_code, "%market capitalization%"),
        )
        credit_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (credit_code, "%domestic credit to private sector%"),
        )

        mkt_vals = [r["value"] for r in mkt_rows if r["value"] is not None]
        credit_vals = [r["value"] for r in credit_rows if r["value"] is not None]

        if not mkt_vals and not credit_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for market cap or private credit",
            }

        mkt = mkt_vals[0] if mkt_vals else 0.0
        credit = credit_vals[0] if credit_vals else 0.0
        mkt_trend = round(mkt_vals[0] - mkt_vals[-1], 3) if len(mkt_vals) > 1 else None

        # Financial depth composite: avg of market cap and credit (both % GDP)
        depth = (mkt + credit) / 2.0

        # Invert: deeper financial markets = lower stress for impact investing
        if depth >= 150:
            score = 10.0
        elif depth >= 80:
            score = 10.0 + (150.0 - depth) * 0.29
        elif depth >= 40:
            score = 30.0 + (80.0 - depth) * 0.5
        elif depth >= 15:
            score = 50.0 + (40.0 - depth) * 0.8
        else:
            score = min(100.0, 70.0 + (15.0 - depth) * 2.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "stock_market_cap_gdp_pct": round(mkt, 2),
                "private_credit_gdp_pct": round(credit, 2),
                "financial_depth_composite": round(depth, 2),
                "market_cap_trend": mkt_trend,
                "n_obs_market": len(mkt_vals),
                "n_obs_credit": len(credit_vals),
            },
        }
