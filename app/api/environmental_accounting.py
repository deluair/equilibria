"""L-EA Environmental Accounting API routes."""
from __future__ import annotations
from typing import Any
from fastapi import APIRouter, Response
from app.db import get_db, release_db
from app.layers.environmental_accounting import ALL_MODULES

router = APIRouter(prefix="/environmental_accounting", tags=["L-EA Environmental Accounting"])
CACHE_1H = "public, max-age=3600, s-maxage=86400"

@router.get("/score")
async def environmental_accounting_composite_score(response: Response) -> dict[str, Any]:
    """L-EA composite score across all environmental accounting indicators."""
    response.headers["Cache-Control"] = CACHE_1H
    db = await get_db()
    try:
        results = []
        for ModuleClass in ALL_MODULES:
            r = await ModuleClass().run(db)
            results.append(r)
        scores = [r["score"] for r in results if r.get("score") is not None]
        avg = round(sum(scores) / len(scores), 2) if scores else None
        return {"layer": "environmental_accounting", "score": avg, "modules_run": len(results), "modules_scored": len(scores), "results": results}
    finally:
        await release_db(db)
