"""Academic Output Index module.

Measures scientific journal article output per million population using
IP.JRN.ARTC.SC (Scientific and technical journal articles).

Sources: World Bank WDI
"""

from __future__ import annotations

from app.layers.base import LayerBase


class AcademicOutputIndex(LayerBase):
    layer_id = "lKE"
    name = "Academic Output Index"

    # Frontier benchmark: ~2000 articles per million (Switzerland/Sweden level)
    BENCHMARK_PER_M = 2000.0

    async def compute(self, db, **kwargs) -> dict:
        art_code = "IP.JRN.ARTC.SC"
        art_name = "Scientific and technical journal articles"
        pop_code = "SP.POP.TOTL"
        pop_name = "Population, total"

        art_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (art_code, f"%{art_name}%"),
        )
        pop_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (pop_code, f"%{pop_name}%"),
        )

        art_vals = [float(r["value"]) for r in art_rows if r["value"] is not None] if art_rows else []
        pop_vals = [float(r["value"]) for r in pop_rows if r["value"] is not None] if pop_rows else []

        if not art_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for IP.JRN.ARTC.SC"}

        articles = art_vals[0]

        if pop_vals and pop_vals[0] > 0:
            articles_per_m = (articles / pop_vals[0]) * 1_000_000
            score = max(0.0, min(100.0, (1.0 - articles_per_m / self.BENCHMARK_PER_M) * 100.0))
            return {
                "score": round(score, 1),
                "articles_per_million": round(articles_per_m, 2),
                "total_articles": articles,
                "benchmark_per_million": self.BENCHMARK_PER_M,
            }

        # Fallback: use absolute articles with a rough absolute benchmark
        abs_benchmark = 500_000.0
        score = max(0.0, min(100.0, (1.0 - articles / abs_benchmark) * 100.0))
        return {
            "score": round(score, 1),
            "total_articles": articles,
            "note": "population unavailable, scored on absolute count",
        }
