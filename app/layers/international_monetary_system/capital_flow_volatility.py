"""Capital flow volatility: instability of portfolio and FDI flows.

Sudden stops and reversals of capital flows are a defining feature of
international monetary crises (Calvo, 1998). High volatility in FDI and
portfolio investment signals fragility in the external financing position.
This module uses WDI BX.KLT.DINV.WD.GD.ZS (FDI net inflows as % of GDP)
and computes volatility as a measure of capital account instability.

Obstfeld & Taylor (2004): capital flow volatility amplifies business cycle
fluctuations and increases crisis probability.

Score: stable, moderate FDI inflows -> STABLE; high volatility or sudden
reversal -> STRESS/CRISIS.
"""

from __future__ import annotations

import statistics

from app.layers.base import LayerBase


class CapitalFlowVolatility(LayerBase):
    layer_id = "lMS"
    name = "Capital Flow Volatility"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        fdi_code = "BX.KLT.DINV.WD.GD.ZS"
        fdi_name = "Foreign direct investment"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (fdi_code, f"%{fdi_name}%"),
        )
        # Portfolio flows proxy
        port_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("BX.PEF.TOTL.CD.WD", "%portfolio equity%"),
        )

        fdi_vals = [r["value"] for r in rows if r["value"] is not None]
        port_vals = [r["value"] for r in port_rows if r["value"] is not None]

        if not fdi_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for BX.KLT.DINV.WD.GD.ZS",
            }

        latest = fdi_vals[0]
        vol = round(statistics.stdev(fdi_vals), 3) if len(fdi_vals) > 2 else abs(latest)
        trend = round(fdi_vals[0] - fdi_vals[-1], 3) if len(fdi_vals) > 1 else None

        # Score: high volatility = more stress; negative FDI (sudden stop) = crisis
        if latest < 0:
            base = 75.0
        elif vol < 1:
            base = 10.0
        elif vol < 2:
            base = 20.0
        elif vol < 4:
            base = 35.0 + (vol - 2) * 7.5
        elif vol < 8:
            base = 50.0 + (vol - 4) * 5.0
        else:
            base = min(95.0, 70.0 + (vol - 8) * 2.5)

        # Declining trend premium
        if trend is not None and trend < -2:
            base = min(100.0, base + 10.0)

        score = round(base, 2)
        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "fdi_inflows_gdp_pct": round(latest, 2),
                "fdi_volatility": vol,
                "trend_change": trend,
                "portfolio_obs": len(port_vals),
                "n_obs": len(fdi_vals),
                "sudden_stop": latest < 0,
            },
        }
