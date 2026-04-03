"""Stock market development for capital markets layer.

Market capitalization as % of GDP (CM.MKT.LCAP.GD.ZS) and stock market
turnover ratio (CM.MKT.TRNR) measure capital market breadth and activity.
High market cap/GDP indicates developed equity markets; high turnover
signals active secondary markets.

Score (0-100): stress-oriented. Low market cap and low turnover push toward CRISIS.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class StockMarketDevelopment(LayerBase):
    layer_id = "lCK"
    name = "Stock Market Development"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        lookback = kwargs.get("lookback_years", 10)

        rows = await db.fetch_all(
            """
            SELECT ds.indicator_code, dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.indicator_code IN ('CM.MKT.LCAP.GD.ZS', 'CM.MKT.TRNR')
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
                "error": "no stock market data",
            }

        by_code: dict[str, list[float]] = {}
        for r in rows:
            by_code.setdefault(r["indicator_code"], []).append(float(r["value"]))

        mktcap_vals = np.array(by_code["CM.MKT.LCAP.GD.ZS"]) if "CM.MKT.LCAP.GD.ZS" in by_code else None
        turnover_vals = np.array(by_code["CM.MKT.TRNR"]) if "CM.MKT.TRNR" in by_code else None

        if mktcap_vals is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "market cap/GDP series not found",
            }

        mktcap_latest = float(mktcap_vals[-1])
        mktcap_mean = float(np.mean(mktcap_vals))

        turnover_latest = float(turnover_vals[-1]) if turnover_vals is not None else None
        turnover_mean = float(np.mean(turnover_vals)) if turnover_vals is not None else None

        # Score: low market cap = stress. Threshold: >100% GDP = well developed.
        mktcap_score = float(np.clip(100.0 - mktcap_latest, 0.0, 100.0))

        # Turnover component: low turnover = illiquid markets
        if turnover_latest is not None:
            turnover_score = float(np.clip(100.0 - turnover_latest, 0.0, 100.0))
            score = round(0.6 * mktcap_score + 0.4 * turnover_score, 2)
        else:
            score = round(mktcap_score, 2)

        return {
            "score": score,
            "country": country,
            "market_cap_pct_gdp": {
                "latest": round(mktcap_latest, 2),
                "mean": round(mktcap_mean, 2),
                "observations": len(mktcap_vals),
            },
            "turnover_ratio": {
                "latest": round(turnover_latest, 2) if turnover_latest is not None else None,
                "mean": round(turnover_mean, 2) if turnover_mean is not None else None,
            },
            "development_level": (
                "underdeveloped" if mktcap_latest < 20
                else "emerging" if mktcap_latest < 50
                else "intermediate" if mktcap_latest < 100
                else "developed"
            ),
        }
