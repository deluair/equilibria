"""Stock market development and capital market depth.

Market capitalization of listed companies (% GDP, CM.MKT.LCAP.GD.ZS) is the
primary indicator. If unavailable, domestic credit to private sector
(FS.AST.DOMS.GD.ZS) serves as proxy. Low market cap indicates underdeveloped
capital markets and higher dependence on bank intermediation.

Score (0-100): clip(100 - market_cap_pct_gdp, 0, 100).
Low market cap pushes toward CRISIS.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class StockMarketDevelopment(LayerBase):
    layer_id = "l7"
    name = "Stock Market Development"

    _INDICATORS = [
        ("CM.MKT.LCAP.GD.ZS", "market_cap"),
        ("FS.AST.DOMS.GD.ZS", "domestic_credit"),
    ]

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        lookback = kwargs.get("lookback_years", 10)

        rows = await db.fetch_all(
            """
            SELECT ds.indicator_code, dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.indicator_code IN ('CM.MKT.LCAP.GD.ZS', 'FS.AST.DOMS.GD.ZS')
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
                "error": "no capital market depth data",
            }

        by_indicator: dict[str, list[float]] = {}
        for r in rows:
            by_indicator.setdefault(r["indicator_code"], []).append(float(r["value"]))

        indicator_used = None
        metric_label = None
        values = None
        for code, label in self._INDICATORS:
            if code in by_indicator and by_indicator[code]:
                values = np.array(by_indicator[code])
                indicator_used = code
                metric_label = label
                break

        if values is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no usable capital market series found",
            }

        latest = float(values[-1])
        mean_val = float(np.mean(values))

        # For market cap: high = developed. For credit proxy, high also = deeper.
        # Score = stress, so invert.
        score = float(np.clip(100.0 - latest, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "country": country,
            "capital_market_depth": {
                "metric": metric_label,
                "indicator": indicator_used,
                "latest_pct_gdp": round(latest, 2),
                "mean_pct_gdp": round(mean_val, 2),
                "observations": len(values),
            },
            "development_level": (
                "underdeveloped" if latest < 20
                else "emerging" if latest < 50
                else "intermediate" if latest < 100
                else "developed"
            ),
            "proxy_used": indicator_used != "CM.MKT.LCAP.GD.ZS",
        }
