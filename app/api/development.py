"""L4 Development API routes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Response

from app.db import get_db, release_db
from app.layers.development import (
    ALL_MODULES,
    BetaConvergence,
    HDIDecomposition,
    InstitutionalQuality,
    MultidimensionalPoverty,
)

router = APIRouter(prefix="/development", tags=["development"])

CACHE_1H = "public, max-age=3600, s-maxage=86400"


@router.get("/convergence")
async def convergence(
    response: Response,
) -> dict[str, Any]:
    """Beta and sigma convergence analysis."""
    response.headers["Cache-Control"] = CACHE_1H
    db = await get_db()
    try:
        return await BetaConvergence().run(db)
    finally:
        await release_db(db)


@router.get("/poverty/{iso3}")
async def poverty_analysis(
    iso3: str,
    response: Response,
) -> dict[str, Any]:
    """Poverty analysis (trap detection, MPI, headcount)."""
    response.headers["Cache-Control"] = CACHE_1H
    iso3 = iso3.upper()
    db = await get_db()
    try:
        return await MultidimensionalPoverty().run(db, country=iso3)
    finally:
        await release_db(db)


@router.get("/institutions/{iso3}")
async def institutional_quality(
    iso3: str,
    response: Response,
) -> dict[str, Any]:
    """Institutional quality analysis (IV with settler mortality, legal origins)."""
    response.headers["Cache-Control"] = CACHE_1H
    iso3 = iso3.upper()
    db = await get_db()
    try:
        return await InstitutionalQuality().run(db, country=iso3)
    finally:
        await release_db(db)


@router.get("/hdi/{iso3}")
async def hdi_decomposition(
    iso3: str,
    response: Response,
) -> dict[str, Any]:
    """HDI decomposition and dynamics."""
    response.headers["Cache-Control"] = CACHE_1H
    iso3 = iso3.upper()
    db = await get_db()
    try:
        return await HDIDecomposition().run(db, country=iso3)
    finally:
        await release_db(db)


@router.get("/score")
async def development_composite_score(
    response: Response,
) -> dict[str, Any]:
    """Layer 4 composite score across all development indicators."""
    response.headers["Cache-Control"] = CACHE_1H
    db = await get_db()
    try:
        scores = []
        for cls in ALL_MODULES:
            result = await cls().run(db)
            if result.get("score") is not None:
                scores.append(result["score"])
        if not scores:
            raise HTTPException(status_code=503, detail="No development scores available")
        composite = sum(scores) / len(scores)
        return {"layer": "development", "score": composite, "n_modules": len(scores)}
    finally:
        await release_db(db)
