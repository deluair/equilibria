"""Maritime trade dependence: trade openness and logistics quality as ocean-channel proxies.

Combines trade openness (NE.TRD.GNFS.ZS) with logistics performance (LP.LPI.OVRL.XQ)
to assess how dependent a country is on maritime channels and how well it manages
that dependence. High trade + poor logistics = elevated risk.

Sources: World Bank WDI (NE.TRD.GNFS.ZS, LP.LPI.OVRL.XQ)
"""

from __future__ import annotations

from app.layers.base import LayerBase


class MaritimeTradeDepence(LayerBase):
    layer_id = "lOE"
    name = "Maritime Trade Dependence"

    async def compute(self, db, **kwargs) -> dict:
        trade_code = "NE.TRD.GNFS.ZS"
        trade_name = "trade % GDP"
        trade_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (trade_code, f"%{trade_name}%"),
        )

        lpi_code = "LP.LPI.OVRL.XQ"
        lpi_name = "logistics performance"
        lpi_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (lpi_code, f"%{lpi_name}%"),
        )

        if not trade_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No trade openness data found",
            }

        trade_vals = [row["value"] for row in trade_rows if row["value"] is not None]
        lpi_vals = [row["value"] for row in lpi_rows if row["value"] is not None]

        if not trade_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "All trade openness rows have null values",
            }

        trade_latest = float(trade_vals[0])
        lpi_latest = float(lpi_vals[0]) if lpi_vals else None

        # High trade openness = high maritime dependence
        # LPI scale 1-5; lower = worse logistics = higher cost/risk
        # Score: penalty for high trade + poor logistics
        trade_norm = min(trade_latest / 150.0, 1.0)  # 150% trade/GDP = fully exposed

        if lpi_latest is not None:
            lpi_norm = 1.0 - (lpi_latest - 1.0) / 4.0  # invert: lower LPI = higher risk
        else:
            lpi_norm = 0.5  # neutral fallback

        score = round(min(100.0, (trade_norm * 60.0) + (lpi_norm * 40.0)), 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "trade_pct_gdp": round(trade_latest, 2),
                "lpi_score": round(lpi_latest, 3) if lpi_latest is not None else None,
                "trade_norm": round(trade_norm, 3),
                "lpi_risk_norm": round(lpi_norm, 3),
                "n_trade_obs": len(trade_vals),
                "n_lpi_obs": len(lpi_vals),
            },
        }
