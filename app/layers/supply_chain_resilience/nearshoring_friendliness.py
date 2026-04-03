"""Nearshoring friendliness: business environment quality composite.

Uses IC.BUS.EASE.XQ (ease of doing business score, 0-100) and
RQ.EST (regulatory quality estimate, -2.5 to +2.5, WGI). Countries with
better business environments are more attractive for nearshoring investment.

Methodology:
    Fetch latest available value for each indicator.
    Normalize regulatory quality: rq_norm = clip((rq + 2.5) / 5 * 100, 0, 100).
    If both available: composite = (ease + rq_norm) / 2.
    Score is inverted: score = clip(100 - composite, 0, 100).

    Composite = 100 (best env): score = 0 (most nearshoring-friendly).
    Composite = 0 (worst env): score = 100 (least nearshoring-friendly).

Score (0-100): Higher score indicates less nearshoring-friendly environment.

References:
    World Bank Doing Business IC.BUS.EASE.XQ.
    World Bank WGI Regulatory Quality RQ.EST.
    Reshoring Institute (2022). "What Drives Nearshoring Decisions?"
"""

from __future__ import annotations

from app.layers.base import LayerBase

_EDB_CODE = "IC.BUS.EASE.XQ"
_RQ_CODE = "RQ.EST"


class NearshoringFriendliness(LayerBase):
    layer_id = "lSR"
    name = "Nearshoring Friendliness"

    async def _fetch_latest(self, db, code: str, name: str) -> float | None:
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        for r in rows:
            if r["value"] is not None:
                return float(r["value"])
        return None

    async def compute(self, db, **kwargs) -> dict:
        edb = await self._fetch_latest(db, _EDB_CODE, "ease of doing business")
        rq = await self._fetch_latest(db, _RQ_CODE, "regulatory quality")

        if edb is None and rq is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for IC.BUS.EASE.XQ or RQ.EST",
            }

        components: list[float] = []
        if edb is not None:
            components.append(max(0.0, min(100.0, edb)))
        if rq is not None:
            rq_norm = max(0.0, min(100.0, (rq + 2.5) / 5.0 * 100.0))
            components.append(rq_norm)

        composite = sum(components) / len(components)
        score = float(min(max(100.0 - composite, 0.0), 100.0))

        return {
            "score": round(score, 2),
            "ease_of_business_score": round(edb, 2) if edb is not None else None,
            "regulatory_quality_raw": round(rq, 3) if rq is not None else None,
            "composite_friendliness": round(composite, 2),
            "edb_indicator": _EDB_CODE,
            "rq_indicator": _RQ_CODE,
        }
