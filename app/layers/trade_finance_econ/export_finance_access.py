"""Export Finance Access module.

Proxy for export finance access using FDI inflows and private credit depth.
FDI inflows signal international financial integration (and access to foreign
trade finance), while deep domestic credit markets enable local export financing.
Low values on both dimensions indicate constrained export finance access.

Sources: WDI BX.KLT.DINV.WD.GD.ZS (FDI net inflows % GDP),
         WDI FS.AST.PRVT.GD.ZS (domestic credit to private sector % GDP)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ExportFinanceAccess(LayerBase):
    layer_id = "lTF"
    name = "Export Finance Access"

    async def compute(self, db, **kwargs) -> dict:
        fdi_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            ("BX.KLT.DINV.WD.GD.ZS", "%FDI%inflows%GDP%"),
        )
        credit_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            ("FS.AST.PRVT.GD.ZS", "%private%credit%GDP%"),
        )

        if not fdi_rows and not credit_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no export finance proxy data"}

        fdi_pct = float(fdi_rows[0]["value"]) if fdi_rows else 0.0
        credit_pct = float(credit_rows[0]["value"]) if credit_rows else 0.0

        # Access index: FDI up to 10% GDP + credit up to 100% GDP normalized
        fdi_score = float(np.clip(fdi_pct / 10.0, 0, 1)) * 40
        credit_score = float(np.clip(credit_pct / 100.0, 0, 1)) * 60
        access_index = fdi_score + credit_score  # 0-100, higher = better access

        # Stress score: invert (low access = high stress)
        score = float(np.clip(100 - access_index, 0, 100))

        return {
            "score": round(score, 2),
            "fdi_inflows_pct_gdp": round(fdi_pct, 3),
            "private_credit_pct_gdp": round(credit_pct, 2),
            "access_index": round(access_index, 2),
            "interpretation": "Low FDI inflows and shallow credit markets proxy constrained export finance access",
        }
