"""Patent Productivity module.

Measures patent applications per million population using IP.PAT.RESD.
Higher patent density signals stronger innovation output.

Sources: World Bank WDI
"""

from __future__ import annotations

from app.layers.base import LayerBase


class PatentProductivity(LayerBase):
    layer_id = "lKE"
    name = "Patent Productivity"

    # Frontier benchmark: ~1000 applications per million (Korea/Japan level)
    BENCHMARK_PER_M = 1000.0

    async def compute(self, db, **kwargs) -> dict:
        patent_code = "IP.PAT.RESD"
        patent_name = "Patent applications, residents"
        pop_code = "SP.POP.TOTL"
        pop_name = "Population, total"

        pat_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (patent_code, f"%{patent_name}%"),
        )
        pop_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (pop_code, f"%{pop_name}%"),
        )

        if not pat_rows or not pop_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data for patent productivity"}

        pat_vals = [float(r["value"]) for r in pat_rows if r["value"] is not None]
        pop_vals = [float(r["value"]) for r in pop_rows if r["value"] is not None]
        if not pat_vals or not pop_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "null values only"}

        patents_per_m = (pat_vals[0] / pop_vals[0]) * 1_000_000
        score = max(0.0, min(100.0, (1.0 - patents_per_m / self.BENCHMARK_PER_M) * 100.0))

        return {
            "score": round(score, 1),
            "patents_per_million": round(patents_per_m, 2),
            "patent_applications": pat_vals[0],
            "population": pop_vals[0],
            "benchmark_per_million": self.BENCHMARK_PER_M,
        }
