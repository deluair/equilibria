"""Accessibility infrastructure gap: public infrastructure accessibility score.

Physical infrastructure -- roads, public transport, buildings -- determines
whether people with disabilities can participate in economic and social life.
Paved roads share (IS.ROD.PAVE.ZS) proxies basic infrastructure quality and
reach; high unpaved-road share signals a rural mobility barrier that
disproportionately excludes mobility-impaired individuals.

Score: high paved roads -> STABLE accessible infrastructure baseline.
Low paved roads -> CRISIS inaccessible built environment.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class AccessibilityInfrastructureGap(LayerBase):
    layer_id = "lDI"
    name = "Accessibility Infrastructure Gap"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        road_code = "IS.ROD.PAVE.ZS"

        road_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (road_code, "%paved road%"),
        )

        road_vals = [r["value"] for r in road_rows if r["value"] is not None]

        if not road_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for IS.ROD.PAVE.ZS"}

        paved_pct = road_vals[0]
        # Gap is the inverse: unpaved share signals accessibility deficit
        unpaved_pct = max(0.0, 100.0 - paved_pct)

        if unpaved_pct < 10:
            score = 5.0 + unpaved_pct * 1.5
        elif unpaved_pct < 30:
            score = 20.0 + (unpaved_pct - 10) * 1.25
        elif unpaved_pct < 60:
            score = 45.0 + (unpaved_pct - 30) * 0.833
        else:
            score = min(100.0, 70.0 + (unpaved_pct - 60) * 0.75)

        trend = round(road_vals[0] - road_vals[-1], 3) if len(road_vals) > 1 else None

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "paved_roads_pct": round(paved_pct, 2),
                "unpaved_roads_pct": round(unpaved_pct, 2),
                "paved_trend": trend,
                "n_obs": len(road_vals),
            },
        }
