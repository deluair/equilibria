"""Market liquidity index.

Liquidity is captured through stock market turnover ratio (CM.MKT.TRNR),
which approximates bid-ask spread and market depth. High turnover indicates
liquid, active markets where securities can be bought/sold without large
price impact.

Score (0-100): low turnover = illiquid = high stress.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MarketLiquidityIndex(LayerBase):
    layer_id = "lCK"
    name = "Market Liquidity Index"

    # Turnover thresholds (World Bank classification benchmarks)
    LOW_LIQUIDITY_THRESHOLD = 10.0   # % - very illiquid
    HIGH_LIQUIDITY_THRESHOLD = 100.0  # % - highly liquid

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        lookback = kwargs.get("lookback_years", 10)

        rows = await db.fetch_all(
            """
            SELECT ds.indicator_code, dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.indicator_code IN ('CM.MKT.TRNR', 'CM.MKT.LCAP.GD.ZS')
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
                "error": "no market liquidity data",
            }

        by_code: dict[str, list[float]] = {}
        for r in rows:
            by_code.setdefault(r["indicator_code"], []).append(float(r["value"]))

        if "CM.MKT.TRNR" not in by_code:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "turnover ratio series not found",
            }

        turnover_vals = np.array(by_code["CM.MKT.TRNR"])
        turnover_latest = float(turnover_vals[-1])
        turnover_mean = float(np.mean(turnover_vals))
        turnover_trend = float(turnover_vals[-1] - turnover_vals[0]) if len(turnover_vals) > 1 else 0.0

        # Amihud-style illiquidity: low turnover -> high illiquidity -> high stress
        score = float(np.clip(
            100.0 * (1.0 - turnover_latest / self.HIGH_LIQUIDITY_THRESHOLD),
            0.0, 100.0,
        ))

        # Market cap size context
        mktcap_latest = None
        if "CM.MKT.LCAP.GD.ZS" in by_code:
            mc_vals = by_code["CM.MKT.LCAP.GD.ZS"]
            mktcap_latest = round(float(mc_vals[-1]), 2)

        return {
            "score": round(score, 2),
            "country": country,
            "turnover_ratio": {
                "latest_pct": round(turnover_latest, 2),
                "mean_pct": round(turnover_mean, 2),
                "trend_pp": round(turnover_trend, 2),
                "observations": len(turnover_vals),
            },
            "market_cap_pct_gdp": mktcap_latest,
            "liquidity_level": (
                "very_illiquid" if turnover_latest < self.LOW_LIQUIDITY_THRESHOLD
                else "illiquid" if turnover_latest < 30
                else "moderate" if turnover_latest < self.HIGH_LIQUIDITY_THRESHOLD
                else "liquid"
            ),
        }
