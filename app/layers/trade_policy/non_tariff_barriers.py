"""Non-Tariff Barriers module.

Proxies NTB burden using regulatory quality and import costs.
High import cost combined with low regulatory quality signals
substantial non-tariff barrier burden.

Score = clip(import_cost / 1000 * 20 + max(0, 50 - rq * 20) * 0.3, 0, 100)

Sources: WDI
  RQ.EST      - Regulatory Quality: Estimate (World Governance Indicators)
  IC.IMP.COST.CD - Cost to import: border compliance (USD)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class NonTariffBarriers(LayerBase):
    layer_id = "lTP"
    name = "Non-Tariff Barriers"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rq_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'RQ.EST'
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        cost_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IC.IMP.COST.CD'
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rq_rows and not cost_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no NTB proxy data available"}

        rq = None
        if rq_rows:
            rq_vals = [float(r["value"]) for r in rq_rows if r["value"] is not None]
            if rq_vals:
                rq = float(np.mean(rq_vals[:3]))

        import_cost = None
        if cost_rows:
            cost_vals = [float(r["value"]) for r in cost_rows if r["value"] is not None]
            if cost_vals:
                import_cost = float(np.mean(cost_vals[:3]))

        if rq is None and import_cost is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "all proxy values null"}

        rq_val = rq if rq is not None else 0.0
        cost_val = import_cost if import_cost is not None else 500.0  # WB average fallback

        score = float(np.clip(cost_val / 1000 * 20 + max(0, 50 - rq_val * 20) * 0.3, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "regulatory_quality_estimate": round(rq_val, 4) if rq is not None else None,
            "import_cost_usd": round(cost_val, 2) if import_cost is not None else None,
            "ntb_burden": "low" if score < 25 else "moderate" if score < 50 else "high",
        }
