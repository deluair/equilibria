"""Export Credit Insurance module.

Proxy for export credit insurance demand and capacity, combining FDI
inflows (integration with global financial networks that offer ECA
products) and regulatory quality (institutional environment enabling
ECA operations and private credit insurance markets).

Sources: WDI BX.KLT.DINV.WD.GD.ZS (FDI net inflows % GDP),
         WGI RQ.EST (Regulatory Quality estimate, -2.5 to +2.5)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ExportCreditInsurance(LayerBase):
    layer_id = "lTF"
    name = "Export Credit Insurance"

    async def compute(self, db, **kwargs) -> dict:
        fdi_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            ("BX.KLT.DINV.WD.GD.ZS", "%FDI%inflows%GDP%"),
        )
        rq_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            ("RQ.EST", "%regulatory%quality%estimate%"),
        )

        if not fdi_rows and not rq_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no ECA proxy data"}

        fdi_pct = float(fdi_rows[0]["value"]) if fdi_rows else 0.0
        rq_val = float(rq_rows[0]["value"]) if rq_rows else 0.0

        # FDI norm: 0-5% GDP is typical range for integration signal
        fdi_norm = float(np.clip(fdi_pct / 5.0, 0, 1))
        # Regulatory quality: -2.5..+2.5 => 0..1
        rq_norm = float(np.clip((rq_val + 2.5) / 5.0, 0, 1))

        eca_capacity = (0.4 * fdi_norm + 0.6 * rq_norm) * 100  # 0-100, higher = better capacity

        # Stress score: low ECA capacity = high stress
        score = float(np.clip(100 - eca_capacity, 0, 100))

        return {
            "score": round(score, 2),
            "fdi_inflows_pct_gdp": round(fdi_pct, 3) if fdi_rows else None,
            "regulatory_quality_est": round(rq_val, 3) if rq_rows else None,
            "eca_capacity_index": round(eca_capacity, 2),
            "interpretation": "Low FDI integration and weak regulatory quality reduce export credit insurance availability",
        }
