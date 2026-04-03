"""L2 Macro API routes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Response

router = APIRouter(prefix="/macro", tags=["macro"])

CACHE_1H = "public, max-age=3600, s-maxage=86400"


@router.get("/gdp/{iso3}")
async def gdp_decomposition(
    iso3: str,
    response: Response,
) -> dict[str, Any]:
    """GDP decomposition (expenditure, income, production sides)."""
    response.headers["Cache-Control"] = CACHE_1H
    iso3 = iso3.upper()
    raise HTTPException(status_code=501, detail="GDP decomposition not yet implemented")


@router.get("/phillips")
async def phillips_curve(
    response: Response,
) -> dict[str, Any]:
    """Phillips curve estimation (traditional, expectations-augmented, NKPC)."""
    response.headers["Cache-Control"] = CACHE_1H
    raise HTTPException(status_code=501, detail="Phillips curve not yet implemented")


@router.get("/taylor")
async def taylor_rule(
    response: Response,
) -> dict[str, Any]:
    """Taylor rule analysis and deviation tracking."""
    response.headers["Cache-Control"] = CACHE_1H
    raise HTTPException(status_code=501, detail="Taylor rule not yet implemented")


@router.get("/cycle")
async def business_cycle(
    response: Response,
) -> dict[str, Any]:
    """Business cycle indicators (HP filter, Hamilton filter, BN decomposition)."""
    response.headers["Cache-Control"] = CACHE_1H
    raise HTTPException(status_code=501, detail="Business cycle not yet implemented")


@router.get("/fci")
async def financial_conditions_index(
    response: Response,
) -> dict[str, Any]:
    """Financial conditions index (PCA-based)."""
    response.headers["Cache-Control"] = CACHE_1H
    raise HTTPException(status_code=501, detail="FCI not yet implemented")


@router.get("/recession-probability")
async def recession_probability(
    response: Response,
) -> dict[str, Any]:
    """Recession probability model (probit)."""
    response.headers["Cache-Control"] = CACHE_1H
    raise HTTPException(status_code=501, detail="Recession probability not yet implemented")


@router.get("/score")
async def macro_composite_score(
    response: Response,
) -> dict[str, Any]:
    """Layer 2 composite score across all macro indicators."""
    response.headers["Cache-Control"] = CACHE_1H
    raise HTTPException(status_code=501, detail="Macro composite score not yet implemented")
