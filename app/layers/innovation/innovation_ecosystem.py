"""Innovation Ecosystem module.

Measures overall ecosystem health for innovation by combining:
- Ease of doing business index (IC.BUS.EASE.XQ) -- business environment
- R&D expenditure as % of GDP (GB.XPD.RSDV.GD.ZS) -- research intensity
- Internet users as % of population (IT.NET.USER.ZS) -- digital infrastructure

All three dimensions are normalized to 0-100 and averaged into an ecosystem
composite. Higher composite = healthier ecosystem. Score is inverted:
high score = stressed ecosystem.

Sources: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class InnovationEcosystem(LayerBase):
    layer_id = "lNV"
    name = "Innovation Ecosystem"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        # (series_id, max_reference_value, higher_is_better)
        series_config = [
            ("IC.BUS.EASE.XQ", 100.0),   # Ease of doing business: 0-100 (higher = easier)
            ("GB.XPD.RSDV.GD.ZS", 5.0),  # R&D: 0-5% GDP
            ("IT.NET.USER.ZS", 100.0),   # Internet: 0-100%
        ]

        labels = ["business_environment", "rnd_intensity", "digital_infrastructure"]

        raw_values: dict[str, float | None] = {}
        for (sid, _) in series_config:
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
        for (sid, max_val), label in zip(series_config, labels):
            raw = raw_values.get(sid)
            components[label] = round(raw, 4) if raw is not None else None
            if raw is not None:
                normed.append(min(100.0, max(0.0, (raw / max_val) * 100.0)))

        if len(normed) < 2:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        ecosystem_composite = float(np.mean(normed))
        score = max(0.0, 100.0 - ecosystem_composite)

        return {
            "score": round(score, 1),
            "country": country,
            "ecosystem_composite": round(ecosystem_composite, 2),
            "components": components,
            "n_dimensions": len(normed),
            "interpretation": "High score = weak innovation ecosystem; low score = strong ecosystem",
        }
