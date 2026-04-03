"""Short-Term Debt Share module.

Measures the share of short-term external debt (original maturity <= 1 year)
in total external debt. A high share amplifies rollover risk and vulnerability
to sudden stops in capital flows.

Methodology:
- Query DT.DOD.DSTC.ZS (short-term debt, % of total external debt).
- Latest available value determines the score.
- Score = clip(value / 0.5, 0, 100): 50% short-term share = max stress.

Sources: World Bank WDI (DT.DOD.DSTC.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ShortTermDebtShare(LayerBase):
    layer_id = "lXD"
    name = "Short-Term Debt Share"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'DT.DOD.DSTC.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no short-term debt share data"}

        latest = next((r for r in rows if r["value"] is not None), None)
        if latest is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "all values null"}

        share = float(latest["value"])
        score = float(np.clip(share / 0.5, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "short_term_debt_pct_total": round(share, 2),
            "reference_date": latest["date"],
            "high_rollover_pressure": share > 25.0,
            "indicators": ["DT.DOD.DSTC.ZS"],
        }
