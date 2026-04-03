"""Knowledge Trade Balance module.

Measures net position in knowledge trade using royalties and license receipts
(BX.GSR.ROYL.CD) versus payments (BM.GSR.ROYL.CD).

A positive balance signals a net knowledge exporter; negative = net importer.
Score: 50 = balanced, <50 = surplus (stronger), >50 = deficit (weaker).

Sources: World Bank WDI
"""

from __future__ import annotations

import math

from app.layers.base import LayerBase


class KnowledgeTradeBalance(LayerBase):
    layer_id = "lKE"
    name = "Knowledge Trade Balance"

    async def compute(self, db, **kwargs) -> dict:
        receipts_code = "BX.GSR.ROYL.CD"
        receipts_name = "Royalties and license fees, receipts"
        payments_code = "BM.GSR.ROYL.CD"
        payments_name = "Royalties and license fees, payments"

        rec_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (receipts_code, f"%{receipts_name}%"),
        )
        pay_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (payments_code, f"%{payments_name}%"),
        )

        rec_vals = [float(r["value"]) for r in rec_rows if r["value"] is not None] if rec_rows else []
        pay_vals = [float(r["value"]) for r in pay_rows if r["value"] is not None] if pay_rows else []

        if not rec_vals and not pay_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no royalty data available"}

        receipts = rec_vals[0] if rec_vals else 0.0
        payments = pay_vals[0] if pay_vals else 0.0
        net = receipts - payments

        # Sigmoid-based score: 50 at balance, decreasing (better) with surplus
        # score = 50 - 50 * tanh(net / scale), scale = 1e9 (1 billion USD)
        scale = 1e9
        score = max(0.0, min(100.0, 50.0 - 50.0 * math.tanh(net / scale)))

        return {
            "score": round(score, 1),
            "royalty_receipts_usd": receipts,
            "royalty_payments_usd": payments,
            "net_knowledge_balance_usd": round(net, 0),
        }
