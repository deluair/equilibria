"""L-DI Disability Economics API routes."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Response

from app.db import get_db, release_db
from app.layers.disability_economics import ALL_MODULES

router = APIRouter(prefix="/disability_economics", tags=["L-DI Disability Economics"])

CACHE_1H = "public, max-age=3600, s-maxage=86400"


@router.get("/score")
async def disability_economics_composite_score(response: Response) -> dict[str, Any]:
    """L-DI composite score across all disability economics indicators."""
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
            "layer": "disability_economics",
            "score": avg,
            "modules_run": len(results),
            "modules_scored": len(scores),
            "results": results,
        }
    finally:
        await release_db(db)
