"""Contagion risk index module.

Trade openness (NE.TRD.GNFS.ZS) and FDI integration (BX.KLT.DINV.WD.GD.ZS)
determine how exposed an economy is to external financial contagion.

Score (0-100): highly open economies with volatile FDI face greater contagion risk.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

TRADE_CODE = "NE.TRD.GNFS.ZS"
TRADE_NAME = "trade of gdp"
FDI_CODE = "BX.KLT.DINV.WD.GD.ZS"
FDI_NAME = "foreign direct investment net inflows"


class ContagionRiskIndex(LayerBase):
    layer_id = "lFC"
    name = "Contagion Risk Index"

    async def compute(self, db, **kwargs) -> dict:
        trade_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (TRADE_CODE, f"%{TRADE_NAME}%"),
        )
        fdi_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (FDI_CODE, f"%{FDI_NAME}%"),
        )

        if not trade_rows and not fdi_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no contagion risk data"}

        trade_vals = [float(r["value"]) for r in trade_rows if r["value"] is not None]
        fdi_vals = [float(r["value"]) for r in fdi_rows if r["value"] is not None]

        trade_latest = trade_vals[0] if trade_vals else None
        fdi_latest = fdi_vals[0] if fdi_vals else None

        # Trade openness: >100% GDP = very open; contagion channel scales with openness
        openness_score = 30.0
        if trade_latest is not None:
            openness_score = float(np.clip((trade_latest - 30.0) * 0.8, 0, 60))

        # FDI level: higher FDI = more cross-border linkages = more contagion channel
        fdi_score = 20.0
        if fdi_latest is not None:
            fdi_score = float(np.clip(fdi_latest * 10.0, 0, 60))

        # FDI volatility penalty
        vol_penalty = 0.0
        if len(fdi_vals) >= 3:
            arr = np.array(fdi_vals)
            mean_v = float(np.mean(arr))
            if abs(mean_v) > 1e-6:
                cov = abs(float(np.std(arr, ddof=1)) / mean_v)
                vol_penalty = float(np.clip(cov * 20.0, 0, 30))

        score = float(np.clip(0.50 * openness_score + 0.30 * fdi_score + 0.20 * vol_penalty, 0, 100))

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "trade_gdp_pct": round(trade_latest, 2) if trade_latest is not None else None,
                "fdi_gdp_pct": round(fdi_latest, 2) if fdi_latest is not None else None,
                "openness_score": round(openness_score, 2),
                "fdi_integration_score": round(fdi_score, 2),
                "volatility_penalty": round(vol_penalty, 2),
            },
        }
