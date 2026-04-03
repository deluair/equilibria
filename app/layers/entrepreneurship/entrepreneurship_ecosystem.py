"""Entrepreneurship Ecosystem module.

Composite measure of ecosystem quality for entrepreneurship, combining:
- IC.BUS.EASE.XQ: Ease of doing business score (0-100, higher = easier)
- IT.NET.USER.ZS: Internet users (% population) -- digital infrastructure
- GB.XPD.RSDV.GD.ZS: R&D expenditure (% GDP) -- knowledge base

A strong entrepreneurship ecosystem combines easy business registration, digital
connectivity, and an active knowledge economy. Weak performance across these
dimensions signals structural barriers to startup formation and growth.

Score: higher score = weaker ecosystem = more stress.

Sources: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class EntrepreneurshipEcosystem(LayerBase):
    layer_id = "lER"
    name = "Entrepreneurship Ecosystem"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        series_config = [
            ("IC.BUS.EASE.XQ", 100.0, "ease_of_business"),
            ("IT.NET.USER.ZS", 100.0, "internet_penetration_pct"),
            ("GB.XPD.RSDV.GD.ZS", 5.0, "rnd_pct_gdp"),
        ]

        raw_values: dict[str, float | None] = {}
        for sid, _, label in series_config:
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
                raw_values[label] = float(np.mean(vals)) if vals else None
            else:
                raw_values[label] = None

        normed: list[float] = []
        for sid, max_val, label in series_config:
            raw = raw_values.get(label)
            if raw is not None:
                normed.append(min(100.0, max(0.0, (raw / max_val) * 100.0)))

        if len(normed) < 2:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient ecosystem data"}

        composite = float(np.mean(normed))
        score = max(0.0, 100.0 - composite)

        return {
            "score": round(score, 1),
            "country": country,
            "ecosystem_composite": round(composite, 2),
            "components": {label: round(v, 4) if v is not None else None for _, _, label in series_config for label, v in [(label, raw_values.get(label))]},
            "n_dimensions": len(normed),
            "interpretation": "High score = weak ecosystem = barriers to entrepreneurship",
        }
