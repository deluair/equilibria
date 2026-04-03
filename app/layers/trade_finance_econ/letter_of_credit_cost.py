"""Letter of Credit Cost module.

Trade documentation cost as a proxy for letter-of-credit friction.
High import and export documentation costs signal burdensome LC processing,
customs procedures, and compliance overhead that raise trade finance costs.

Sources: WDI IC.IMP.COST.CD (cost to import, documentary compliance USD),
         WDI IC.EXP.COST.CD (cost to export, documentary compliance USD)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class LetterOfCreditCost(LayerBase):
    layer_id = "lTF"
    name = "Letter of Credit Cost"

    async def compute(self, db, **kwargs) -> dict:
        import_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            ("IC.IMP.COST.CD", "%cost%import%documentary%"),
        )
        export_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            ("IC.EXP.COST.CD", "%cost%export%documentary%"),
        )

        if not import_rows and not export_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no documentary compliance cost data"}

        import_cost = float(import_rows[0]["value"]) if import_rows else None
        export_cost = float(export_rows[0]["value"]) if export_rows else None

        available = [v for v in [import_cost, export_cost] if v is not None]
        avg_cost = float(np.mean(available))

        # Benchmarks: <50 USD low friction, >500 USD high friction (Doing Business)
        score = float(np.clip((avg_cost - 50) / 450 * 100, 0, 100))

        return {
            "score": round(score, 2),
            "import_documentary_cost_usd": round(import_cost, 2) if import_cost is not None else None,
            "export_documentary_cost_usd": round(export_cost, 2) if export_cost is not None else None,
            "avg_cost_usd": round(avg_cost, 2),
            "interpretation": "Higher documentary compliance costs indicate greater LC and trade finance friction",
        }
