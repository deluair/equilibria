"""L-MO Monetary API routes."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Response

from app.db import get_db, release_db
from app.layers.monetary import ALL_MODULES

router = APIRouter(prefix="/monetary", tags=["L-MO Monetary"])

CACHE_1H = "public, max-age=3600, s-maxage=86400"


@router.get("/score")
async def monetary_composite_score(response: Response) -> dict[str, Any]:
    """L-MO composite score across all monetary indicators."""
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
            "layer": "monetary",
            "score": avg,
            "modules_run": len(results),
            "modules_scored": len(scores),
            "results": results,
        }
    finally:
        await release_db(db)
