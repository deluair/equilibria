"""Knowledge Worker Share module.

Estimates knowledge-intensive workforce using tertiary enrollment (SE.TER.ENRR)
as a proxy for the share of high-skill / high-tech workers.

Sources: World Bank WDI
"""

from __future__ import annotations

from app.layers.base import LayerBase


class KnowledgeWorkerShare(LayerBase):
    layer_id = "lKE"
    name = "Knowledge Worker Share"

    # Frontier benchmark: ~80% tertiary gross enrollment (advanced OECD average)
    BENCHMARK_TERTIARY = 80.0

    async def compute(self, db, **kwargs) -> dict:
        ter_code = "SE.TER.ENRR"
        ter_name = "School enrollment, tertiary"

        ter_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (ter_code, f"%{ter_name}%"),
        )

        if not ter_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for SE.TER.ENRR"}

        vals = [float(r["value"]) for r in ter_rows if r["value"] is not None]
        if not vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "null values only"}

        latest = vals[0]
        # Score: 0 = at/above benchmark (strong), 100 = near 0% (weak)
        score = max(0.0, min(100.0, (1.0 - latest / self.BENCHMARK_TERTIARY) * 100.0))

        return {
            "score": round(score, 1),
            "tertiary_enrollment_pct": round(latest, 2),
            "benchmark_pct": self.BENCHMARK_TERTIARY,
            "n_obs": len(vals),
        }
