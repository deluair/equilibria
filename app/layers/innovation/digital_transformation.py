"""Digital Transformation module.

Measures digital economy readiness across three channels:
- Internet users as % of population (IT.NET.USER.ZS)
- Mobile cellular subscriptions per 100 people (IT.CEL.SETS.P2)
- Account ownership at a financial institution or mobile-money service (FX.OWN.TOTL.ZS)

Low composite score across these three dimensions indicates a digital transformation
gap: limited connectivity, low mobile penetration, and financial exclusion together
prevent full participation in the digital economy.

Score is inverted: high score = large digital transformation gap.

Sources: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class DigitalTransformation(LayerBase):
    layer_id = "lNV"
    name = "Digital Transformation"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        # (series_id, label, max_reference_value)
        series_config = [
            ("IT.NET.USER.ZS", "internet_pct", 100.0),
            ("IT.CEL.SETS.P2", "mobile_per_100", 200.0),  # can exceed 100
            ("FX.OWN.TOTL.ZS", "financial_account_pct", 100.0),
        ]

        raw_values: dict[str, float | None] = {}
        for sid, _, _ in series_config:
            rows = await db.fetch_all(
                """
                SELECT dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                  AND dp.value IS NOT NULL
                ORDER BY dp.date DESC
                LIMIT 5
                """,
                (country, sid),
            )
            if rows:
                vals = [float(r["value"]) for r in rows if r["value"] is not None]
                raw_values[sid] = float(np.mean(vals)) if vals else None
            else:
                raw_values[sid] = None

        normed: list[float] = []
        components: dict[str, float | None] = {}
        for sid, label, max_val in series_config:
            raw = raw_values.get(sid)
            components[label] = round(raw, 4) if raw is not None else None
            if raw is not None:
                normed.append(min(100.0, max(0.0, (raw / max_val) * 100.0)))

        if len(normed) < 2:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        digital_composite = float(np.mean(normed))
        score = max(0.0, 100.0 - digital_composite)

        return {
            "score": round(score, 1),
            "country": country,
            "digital_composite": round(digital_composite, 2),
            "components": components,
            "n_dimensions": len(normed),
            "interpretation": "High score = large digital transformation gap",
        }
