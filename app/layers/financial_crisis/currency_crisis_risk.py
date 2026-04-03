"""Currency crisis risk module.

Reserve adequacy (FI.RES.TOTL.MO) and current account balance (BN.CAB.XOKA.GD.ZS)
as indicators of external vulnerability and currency crisis probability.

Score (0-100): low reserves and persistent CA deficits push toward CRISIS.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

RESERVES_CODE = "FI.RES.TOTL.MO"
RESERVES_NAME = "total reserves months of imports"
CAB_CODE = "BN.CAB.XOKA.GD.ZS"
CAB_NAME = "current account balance"


class CurrencyCrisisRisk(LayerBase):
    layer_id = "lFC"
    name = "Currency Crisis Risk"

    async def compute(self, db, **kwargs) -> dict:
        res_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (RESERVES_CODE, f"%{RESERVES_NAME}%"),
        )
        cab_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (CAB_CODE, f"%{CAB_NAME}%"),
        )

        if not res_rows and not cab_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no currency crisis data"}

        res_vals = [float(r["value"]) for r in res_rows if r["value"] is not None]
        cab_vals = [float(r["value"]) for r in cab_rows if r["value"] is not None]

        res_latest = res_vals[0] if res_vals else None
        cab_latest = cab_vals[0] if cab_vals else None

        # Reserve adequacy: <3 months = crisis threshold (IMF standard), >6 = comfortable
        res_score = 50.0
        if res_latest is not None:
            # Low reserves = high crisis risk
            res_score = float(np.clip((4.0 - res_latest) * 20.0, 0, 100))

        # CA balance: deficit > -5% GDP raises risk, > -10% = severe
        cab_score = 20.0
        if cab_latest is not None:
            cab_score = float(np.clip(-cab_latest * 8.0, 0, 100))

        # Persistent CA deficit: average over available years
        persist_score = 0.0
        if len(cab_vals) >= 3:
            avg_deficit = -float(np.mean(cab_vals[:5]))
            persist_score = float(np.clip(avg_deficit * 5.0, 0, 30))

        score = float(np.clip(0.55 * res_score + 0.35 * cab_score + 0.10 * persist_score, 0, 100))

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "reserves_months_imports": round(res_latest, 2) if res_latest is not None else None,
                "current_account_gdp_pct": round(cab_latest, 2) if cab_latest is not None else None,
                "reserve_score": round(res_score, 2),
                "current_account_score": round(cab_score, 2),
                "persistence_score": round(persist_score, 2),
            },
        }
