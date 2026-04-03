"""Bond market depth analysis.

Domestic bond market size as a share of GDP proxied via domestic credit to
private sector (FS.AST.DOMS.GD.ZS) and outstanding government securities
(GC.DOD.TOTL.GD.ZS). A deep bond market reduces reliance on bank financing
and broadens the corporate funding base.

Score (0-100): stress-oriented. Low bond market depth pushes toward CRISIS.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class BondMarketDepth(LayerBase):
    layer_id = "lCK"
    name = "Bond Market Depth"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        lookback = kwargs.get("lookback_years", 10)

        rows = await db.fetch_all(
            """
            SELECT ds.indicator_code, dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.indicator_code IN ('FS.AST.DOMS.GD.ZS', 'GC.DOD.TOTL.GD.ZS')
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
                "error": "no bond market data",
            }

        by_code: dict[str, list[float]] = {}
        for r in rows:
            by_code.setdefault(r["indicator_code"], []).append(float(r["value"]))

        # Prefer private credit (financial deepening); fall back to government debt
        if "FS.AST.DOMS.GD.ZS" in by_code:
            vals = np.array(by_code["FS.AST.DOMS.GD.ZS"])
            indicator_used = "FS.AST.DOMS.GD.ZS"
            label = "domestic_credit_pct_gdp"
        elif "GC.DOD.TOTL.GD.ZS" in by_code:
            vals = np.array(by_code["GC.DOD.TOTL.GD.ZS"])
            indicator_used = "GC.DOD.TOTL.GD.ZS"
            label = "govt_debt_pct_gdp"
        else:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no usable bond market series",
            }

        latest = float(vals[-1])
        mean_val = float(np.mean(vals))

        # Deep markets have private credit > 100% GDP; penalise shallow ones.
        score = float(np.clip(100.0 - latest, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "country": country,
            "bond_market": {
                "metric": label,
                "indicator": indicator_used,
                "latest_pct_gdp": round(latest, 2),
                "mean_pct_gdp": round(mean_val, 2),
                "observations": len(vals),
            },
            "depth_category": (
                "shallow" if latest < 20
                else "developing" if latest < 60
                else "moderate" if latest < 100
                else "deep"
            ),
        }
