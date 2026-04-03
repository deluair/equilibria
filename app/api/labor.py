"""L3 Labor API routes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Response

from app.db import get_db, release_db
from app.layers.labor import (
    ALL_MODULES,
    BeveridgeCurve,
    LaborMarketTightness,
    MincerWageEquation,
    ReturnsToEducation,
)

router = APIRouter(prefix="/labor", tags=["labor"])

CACHE_1H = "public, max-age=3600, s-maxage=86400"


@router.get("/wages")
async def wage_analysis(
    response: Response,
) -> dict[str, Any]:
    """Wage analysis (Mincer equation, Oaxaca-Blinder decomposition)."""
    response.headers["Cache-Control"] = CACHE_1H
    db = await get_db()
    try:
        return await MincerWageEquation().run(db)
    finally:
        await release_db(db)


@router.get("/education")
async def returns_to_education(
    response: Response,
) -> dict[str, Any]:
    """Returns to education (OLS, IV with distance/compulsory schooling)."""
    response.headers["Cache-Control"] = CACHE_1H
    db = await get_db()
    try:
        return await ReturnsToEducation().run(db)
    finally:
        await release_db(db)


@router.get("/tightness")
async def labor_market_tightness(
    response: Response,
) -> dict[str, Any]:
    """Labor market tightness index."""
    response.headers["Cache-Control"] = CACHE_1H
    db = await get_db()
    try:
        return await LaborMarketTightness().run(db)
    finally:
        await release_db(db)


@router.get("/beveridge")
async def beveridge_curve(
    response: Response,
) -> dict[str, Any]:
    """Beveridge curve (job matching efficiency)."""
    response.headers["Cache-Control"] = CACHE_1H
    db = await get_db()
    try:
        return await BeveridgeCurve().run(db)
    finally:
        await release_db(db)


@router.get("/score")
async def labor_composite_score(
    response: Response,
) -> dict[str, Any]:
    """Layer 3 composite score across all labor indicators."""
    response.headers["Cache-Control"] = CACHE_1H
    db = await get_db()
    try:
        scores = []
        for cls in ALL_MODULES:
            result = await cls().run(db)
            if result.get("score") is not None:
                scores.append(result["score"])
        if not scores:
            raise HTTPException(status_code=503, detail="No labor scores available")
        composite = sum(scores) / len(scores)
        return {"layer": "labor", "score": composite, "n_modules": len(scores)}
    finally:
        await release_db(db)
