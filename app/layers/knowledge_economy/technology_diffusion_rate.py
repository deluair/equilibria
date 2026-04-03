"""Technology Diffusion Rate module.

Measures technology adoption spread using:
- Internet users % (IT.NET.USER.ZS) — general ICT diffusion
- High-tech exports % of manufactured exports (TX.VAL.TECH.MF.ZS) — productive diffusion

Score reflects the average distance from frontier across both dimensions.

Sources: World Bank WDI
"""

from __future__ import annotations

from app.layers.base import LayerBase


class TechnologyDiffusionRate(LayerBase):
    layer_id = "lKE"
    name = "Technology Diffusion Rate"

    BENCHMARKS = {
        "IT.NET.USER.ZS": ("internet_users_pct", 95.0),
        "TX.VAL.TECH.MF.ZS": ("hightech_exports_pct", 30.0),
    }

    async def compute(self, db, **kwargs) -> dict:
        series_names = {
            "IT.NET.USER.ZS": "Individuals using the Internet",
            "TX.VAL.TECH.MF.ZS": "High-technology exports",
        }
        metrics: dict[str, float | None] = {}
        scores: list[float] = []

        for code, (label, benchmark) in self.BENCHMARKS.items():
            name = series_names[code]
            rows = await db.fetch_all(
                "SELECT value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
                "ORDER BY date DESC LIMIT 15",
                (code, f"%{name}%"),
            )
            vals = [float(r["value"]) for r in rows if r["value"] is not None] if rows else []
            if vals:
                v = vals[0]
                metrics[label] = round(v, 3)
                scores.append(max(0.0, min(100.0, (1.0 - v / benchmark) * 100.0)))
            else:
                metrics[label] = None

        if not scores:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no diffusion data available"}

        score = sum(scores) / len(scores)
        return {
            "score": round(score, 1),
            "metrics": metrics,
            "dimensions_scored": len(scores),
        }
