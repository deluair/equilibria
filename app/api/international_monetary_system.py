"""L-MS International Monetary System API routes."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Response

from app.db import get_db, release_db
from app.layers.international_monetary_system import ALL_MODULES

router = APIRouter(prefix="/international_monetary_system", tags=["L-MS International Monetary System"])

CACHE_1H = "public, max-age=3600, s-maxage=86400"


@router.get("/score")
async def international_monetary_system_composite_score(response: Response) -> dict[str, Any]:
    """L-MS composite score across all international monetary system indicators."""
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
            "layer": "international_monetary_system",
            "score": avg,
            "modules_run": len(results),
            "modules_scored": len(scores),
            "results": results,
        }
    finally:
        await release_db(db)
