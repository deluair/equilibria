"""Tacit Knowledge Index module.

Measures depth of embodied human capital using:
- Tertiary enrollment (SE.TER.ENRR) — breadth of higher education
- R&D expenditure % GDP (GB.XPD.RSDV.GD.ZS) — depth of knowledge investment

Together these proxy tacit knowledge accumulation that cannot be easily codified or traded.

Sources: World Bank WDI
"""

from __future__ import annotations

from app.layers.base import LayerBase


class TacitKnowledgeIndex(LayerBase):
    layer_id = "lKE"
    name = "Tacit Knowledge Index"

    SERIES = [
        ("SE.TER.ENRR", "School enrollment, tertiary", 80.0),
        ("GB.XPD.RSDV.GD.ZS", "Research and development expenditure", 3.0),
    ]

    async def compute(self, db, **kwargs) -> dict:
        scores: list[float] = []
        metrics: dict[str, float | None] = {}

        for code, name_frag, benchmark in self.SERIES:
            rows = await db.fetch_all(
                "SELECT value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
                "ORDER BY date DESC LIMIT 15",
                (code, f"%{name_frag}%"),
            )
            vals = [float(r["value"]) for r in rows if r["value"] is not None] if rows else []
            if vals:
                v = vals[0]
                metrics[code] = round(v, 3)
                scores.append(max(0.0, min(100.0, (1.0 - v / benchmark) * 100.0)))
            else:
                metrics[code] = None

        if not scores:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no tacit knowledge data available"}

        score = sum(scores) / len(scores)
        return {
            "score": round(score, 1),
            "metrics": metrics,
            "dimensions_scored": len(scores),
        }
