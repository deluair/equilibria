"""Global Imbalances module.

Measures current account imbalance persistence as a source of global financial
instability. Persistent large deficits signal unsustainable external financing needs;
persistent large surpluses signal demand shortfalls that distort global trade flows
(Obstfeld & Rogoff 2007; Blanchard & Milesi-Ferretti 2012).

Score = clip(abs(CA_pct_GDP) * 8, 0, 100), averaged over available years.
A 12.5% current account imbalance maps to score 100.

Sources: WDI (BN.CAB.XOKA.GD.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class GlobalImbalances(LayerBase):
    layer_id = "lIN"
    name = "Global Imbalances"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'BN.CAB.XOKA.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 15
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no current account data found",
            }

        values = [float(r["value"]) for r in rows if r["value"] is not None]

        if len(values) < 2:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient current account observations",
            }

        latest_ca = values[0]
        avg_ca = float(np.mean(values))

        # Score based on magnitude of imbalance (both deficit and surplus are stress)
        score = float(np.clip(abs(avg_ca) * 8, 0, 100))

        # Sign-split: deficit vs surplus
        deficit = latest_ca < 0
        surplus = latest_ca > 0

        return {
            "score": round(score, 1),
            "country": country,
            "latest_ca_pct_gdp": round(latest_ca, 3),
            "avg_ca_pct_gdp": round(avg_ca, 3),
            "n_obs": len(values),
            "imbalance_type": "deficit" if deficit else ("surplus" if surplus else "balanced"),
        }
