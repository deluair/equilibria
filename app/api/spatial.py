"""L-SL Spatial API routes."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Response

from app.db import get_db, release_db
from app.layers.spatial import ALL_MODULES

router = APIRouter(prefix="/spatial", tags=["L-SL Spatial"])

CACHE_1H = "public, max-age=3600, s-maxage=86400"


@router.get("/score")
async def spatial_composite_score(response: Response) -> dict[str, Any]:
    """L-SL composite score across all spatial indicators."""
    response.headers["Cache-Control"] = CACHE_1H
    db = await get_db()
    try:
        results = []
        for ModuleClass in ALL_MODULES:
            r = await ModuleClass().run(db)
            results.append(r)
        scores = [r["score"] for r in results if r.get("score") is not None]
        avg = round(sum(scores) / len(scores), 2) if scores else None
        return {
            "layer": "spatial",
            "score": avg,
            "modules_run": len(results),
            "modules_scored": len(scores),
            "results": results,
        }
    finally:
        await release_db(db)
