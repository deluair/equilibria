"""Knowledge Inequality module.

Proxies the within-country divide in knowledge access using:
- Digital divide: internet users % (IT.NET.USER.ZS) inverted — low coverage = high inequality
- Education proxy: ratio of tertiary enrollment (SE.TER.ENRR) to primary completion (SE.PRM.CMPT.ZS)
  A wide gap between primary and tertiary signals steep education attrition (knowledge inequality).

Score: higher = greater knowledge inequality.

Sources: World Bank WDI
"""

from __future__ import annotations

from app.layers.base import LayerBase


class KnowledgeInequality(LayerBase):
    layer_id = "lKE"
    name = "Knowledge Inequality"

    async def compute(self, db, **kwargs) -> dict:
        series = {
            "IT.NET.USER.ZS": "Individuals using the Internet",
            "SE.TER.ENRR": "School enrollment, tertiary",
            "SE.PRM.CMPT.ZS": "Primary completion rate",
        }
        vals: dict[str, float | None] = {}

        for code, name_frag in series.items():
            rows = await db.fetch_all(
                "SELECT value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
                "ORDER BY date DESC LIMIT 15",
                (code, f"%{name_frag}%"),
            )
            row_vals = [float(r["value"]) for r in rows if r["value"] is not None] if rows else []
            vals[code] = row_vals[0] if row_vals else None

        scores: list[float] = []

        # Digital divide: low internet penetration = high inequality
        if vals["IT.NET.USER.ZS"] is not None:
            internet_pct = vals["IT.NET.USER.ZS"]
            # Score = 100 - internet_pct (clamped 0-100)
            scores.append(max(0.0, min(100.0, 100.0 - internet_pct)))

        # Education attrition: tertiary/primary ratio
        if vals["SE.TER.ENRR"] is not None and vals["SE.PRM.CMPT.ZS"] is not None:
            primary = vals["SE.PRM.CMPT.ZS"]
            tertiary = vals["SE.TER.ENRR"]
            if primary > 0:
                ratio = tertiary / primary  # 0-1 range ideally
                # Low ratio = high attrition = high inequality
                scores.append(max(0.0, min(100.0, (1.0 - ratio) * 100.0)))

        if not scores:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no knowledge inequality data available"}

        score = sum(scores) / len(scores)
        return {
            "score": round(score, 1),
            "internet_penetration_pct": vals.get("IT.NET.USER.ZS"),
            "tertiary_enrollment_pct": vals.get("SE.TER.ENRR"),
            "primary_completion_pct": vals.get("SE.PRM.CMPT.ZS"),
            "dimensions_scored": len(scores),
        }
