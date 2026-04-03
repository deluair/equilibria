"""Capital market access analysis.

Measures corporate bond issuance and IPO activity as indicators of firms'
ability to access long-term capital. Proxied by new equity issues
(BX.PEF.TOTL.CD.WD for portfolio equity flows) and domestic credit to private
sector growth (FS.AST.DOMS.GD.ZS year-on-year change as proxy for issuance
activity).

Score (0-100): low access = high stress (markets closed to corporates).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CapitalMarketAccess(LayerBase):
    layer_id = "lCK"
    name = "Capital Market Access"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        lookback = kwargs.get("lookback_years", 10)

        rows = await db.fetch_all(
            """
            SELECT ds.indicator_code, dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.indicator_code IN ('BX.PEF.TOTL.CD.WD', 'FS.AST.DOMS.GD.ZS')
              AND ds.country_iso3 = ?
              AND dp.date >= date('now', ?)
            ORDER BY ds.indicator_code, dp.date
            """,
            (country, f"-{lookback} years"),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no capital market access data",
            }

        by_code: dict[str, list[float]] = {}
        for r in rows:
            by_code.setdefault(r["indicator_code"], []).append(float(r["value"]))

        # Portfolio equity inflows (USD, can be negative = outflows)
        portfolio_equity = None
        portfolio_trend = None
        if "BX.PEF.TOTL.CD.WD" in by_code:
            pe_vals = np.array(by_code["BX.PEF.TOTL.CD.WD"])
            portfolio_equity = float(pe_vals[-1])
            portfolio_trend = "inflow" if portfolio_equity > 0 else "outflow"

        # Private credit growth as proxy for corporate issuance activity
        credit_growth = None
        if "FS.AST.DOMS.GD.ZS" in by_code and len(by_code["FS.AST.DOMS.GD.ZS"]) >= 2:
            cr_vals = np.array(by_code["FS.AST.DOMS.GD.ZS"])
            credit_growth = float(cr_vals[-1] - cr_vals[-2])

        if portfolio_equity is None and credit_growth is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data for capital market access",
            }

        # Score: outflows and negative credit growth = stress
        components = []
        if portfolio_equity is not None:
            # Normalise: positive inflow = low stress
            pe_score = float(np.clip(50.0 - portfolio_equity / 1e8, 0.0, 100.0))
            components.append(pe_score)

        if credit_growth is not None:
            # Positive credit growth = access improving = lower stress
            cg_score = float(np.clip(50.0 - credit_growth * 5.0, 0.0, 100.0))
            components.append(cg_score)

        score = round(float(np.mean(components)), 2)

        return {
            "score": score,
            "country": country,
            "portfolio_equity_inflows_usd": (
                round(portfolio_equity, 0) if portfolio_equity is not None else None
            ),
            "portfolio_flow_direction": portfolio_trend,
            "private_credit_growth_pp": (
                round(credit_growth, 2) if credit_growth is not None else None
            ),
            "access_signal": (
                "restricted" if score > 65
                else "constrained" if score > 40
                else "open"
            ),
        }
