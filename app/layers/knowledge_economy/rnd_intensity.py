"""R&D Intensity module.

Measures R&D expenditure as % of GDP (GB.XPD.RSDV.GD.ZS) against a 3% benchmark.
Score reflects distance from the 3% innovation frontier.

Sources: World Bank WDI
"""

from __future__ import annotations

from app.layers.base import LayerBase


class RndIntensity(LayerBase):
    layer_id = "lKE"
    name = "R&D Intensity"

    BENCHMARK = 3.0  # % GDP — OECD innovation frontier threshold

    async def compute(self, db, **kwargs) -> dict:
        code = "GB.XPD.RSDV.GD.ZS"
        name = "Research and development expenditure"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for GB.XPD.RSDV.GD.ZS"}

        vals = [float(r["value"]) for r in rows if r["value"] is not None]
        if not vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "null values only"}

        latest = vals[0]
        # Score: 0 = at/above 3% benchmark (strong), 100 = near 0% (weak)
        score = max(0.0, min(100.0, (1.0 - latest / self.BENCHMARK) * 100.0))

        return {
            "score": round(score, 1),
            "rnd_pct_gdp": round(latest, 3),
            "benchmark_pct": self.BENCHMARK,
            "n_obs": len(vals),
        }
