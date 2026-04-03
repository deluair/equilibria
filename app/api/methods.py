"""L-MT Methods API routes."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Response

from app.db import get_db, release_db
from app.layers.methods import ALL_MODULES

router = APIRouter(prefix="/methods", tags=["L-MT Methods"])

CACHE_1H = "public, max-age=3600, s-maxage=86400"


@router.get("/score")
async def methods_composite_score(response: Response) -> dict[str, Any]:
    """L-MT composite score across all methods indicators."""
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
            "layer": "methods",
            "score": avg,
            "modules_run": len(results),
            "modules_scored": len(scores),
            "results": results,
        }
    finally:
        await release_db(db)
