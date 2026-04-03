"""Innovation Ecosystem Score module.

GII-style composite combining four pillars:
- R&D expenditure % GDP (GB.XPD.RSDV.GD.ZS)
- Patent applications, residents (IP.PAT.RESD) — normalized by population
- Tertiary enrollment (SE.TER.ENRR)
- High-tech exports % manufactured (TX.VAL.TECH.MF.ZS)

Score reflects average distance from frontier across available pillars.

Sources: World Bank WDI
"""

from __future__ import annotations

from app.layers.base import LayerBase


class InnovationEcosystemScore(LayerBase):
    layer_id = "lKE"
    name = "Innovation Ecosystem Score"

    # (indicator_code, name_fragment, benchmark, is_per_million_pop)
    PILLARS = [
        ("GB.XPD.RSDV.GD.ZS", "Research and development expenditure", 3.0, False),
        ("SE.TER.ENRR", "School enrollment, tertiary", 80.0, False),
        ("TX.VAL.TECH.MF.ZS", "High-technology exports", 30.0, False),
    ]
    # Patents handled separately (needs population normalization)
    PATENT_CODE = "IP.PAT.RESD"
    POP_CODE = "SP.POP.TOTL"
    PATENT_BENCHMARK_PER_M = 1000.0

    async def compute(self, db, **kwargs) -> dict:
        scores: list[float] = []
        components: dict[str, float | None] = {}

        for code, name_frag, benchmark, _ in self.PILLARS:
            rows = await db.fetch_all(
                "SELECT value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
                "ORDER BY date DESC LIMIT 15",
                (code, f"%{name_frag}%"),
            )
            vals = [float(r["value"]) for r in rows if r["value"] is not None] if rows else []
            if vals:
                v = vals[0]
                components[code] = round(v, 3)
                scores.append(max(0.0, min(100.0, (1.0 - v / benchmark) * 100.0)))
            else:
                components[code] = None

        # Patent pillar
        pat_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (self.PATENT_CODE, "%Patent applications, residents%"),
        )
        pop_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (self.POP_CODE, "%Population, total%"),
        )
        pat_vals = [float(r["value"]) for r in pat_rows if r["value"] is not None] if pat_rows else []
        pop_vals = [float(r["value"]) for r in pop_rows if r["value"] is not None] if pop_rows else []
        if pat_vals and pop_vals and pop_vals[0] > 0:
            patents_per_m = (pat_vals[0] / pop_vals[0]) * 1_000_000
            components["IP.PAT.RESD_per_million"] = round(patents_per_m, 2)
            scores.append(max(0.0, min(100.0, (1.0 - patents_per_m / self.PATENT_BENCHMARK_PER_M) * 100.0)))
        else:
            components["IP.PAT.RESD_per_million"] = None

        if not scores:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no ecosystem data available"}

        score = sum(scores) / len(scores)
        return {
            "score": round(score, 1),
            "components": components,
            "pillars_scored": len(scores),
        }
