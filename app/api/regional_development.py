"""L-RD Regional Development API routes."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Response

from app.db import get_db, release_db
from app.layers.regional_development import ALL_MODULES

router = APIRouter(prefix="/regional_development", tags=["L-RD Regional Development"])

CACHE_1H = "public, max-age=3600, s-maxage=86400"


@router.get("/score")
async def regional_development_composite_score(response: Response) -> dict[str, Any]:
    """L-RD composite score across all regional_development indicators."""
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
            "layer": "regional_development",
            "score": avg,
            "modules_run": len(results),
            "modules_scored": len(scores),
            "results": results,
        }
    finally:
        await release_db(db)
