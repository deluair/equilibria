"""L2 Macro API routes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Response

from app.db import get_db, release_db
from app.layers.macro import (
    ALL_MODULES,
    BusinessCycle,
    FinancialConditionsIndex,
    RecessionProbability,
)
from app.layers.macro.gdp_decomposition import GDPDecomposition
from app.layers.macro.phillips_curve import PhillipsCurve
from app.layers.macro.taylor_rule import TaylorRule

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
    db = await get_db()
    try:
        module = GDPDecomposition()
        return await module.run(db, country=iso3)
    finally:
        await release_db(db)


@router.get("/phillips")
async def phillips_curve(
    response: Response,
) -> dict[str, Any]:
    """Phillips curve estimation (traditional, expectations-augmented, NKPC)."""
    response.headers["Cache-Control"] = CACHE_1H
    db = await get_db()
    try:
        module = PhillipsCurve()
        return await module.run(db)
    finally:
        await release_db(db)


@router.get("/taylor")
async def taylor_rule(
    response: Response,
) -> dict[str, Any]:
    """Taylor rule analysis and deviation tracking."""
    response.headers["Cache-Control"] = CACHE_1H
    db = await get_db()
    try:
        module = TaylorRule()
        return await module.run(db)
    finally:
        await release_db(db)


@router.get("/cycle")
async def business_cycle(
    response: Response,
) -> dict[str, Any]:
    """Business cycle indicators (HP filter, Hamilton filter, BN decomposition)."""
    response.headers["Cache-Control"] = CACHE_1H
    db = await get_db()
    try:
        module = BusinessCycle()
        return await module.run(db)
    finally:
        await release_db(db)


@router.get("/fci")
async def financial_conditions_index(
    response: Response,
) -> dict[str, Any]:
    """Financial conditions index (PCA-based)."""
    response.headers["Cache-Control"] = CACHE_1H
    db = await get_db()
    try:
        module = FinancialConditionsIndex()
        return await module.run(db)
    finally:
        await release_db(db)


@router.get("/recession-probability")
async def recession_probability(
    response: Response,
) -> dict[str, Any]:
    """Recession probability model (probit)."""
    response.headers["Cache-Control"] = CACHE_1H
    db = await get_db()
    try:
        module = RecessionProbability()
        return await module.run(db)
    finally:
        await release_db(db)


@router.get("/score")
async def macro_composite_score(
    response: Response,
) -> dict[str, Any]:
    """Layer 2 composite score across all macro indicators."""
    response.headers["Cache-Control"] = CACHE_1H
    db = await get_db()
    try:
        results = []
        for cls in ALL_MODULES:
            result = await cls().run(db)
            results.append(result)
        scores = [r["score"] for r in results if r.get("score") is not None]
        avg_score = sum(scores) / len(scores) if scores else None
        return {
            "layer_id": "l2",
            "name": "Macro",
            "score": avg_score,
            "signal": results[0]["signal"] if avg_score is None else _classify(avg_score),
            "modules": results,
        }
    finally:
        await release_db(db)


def _classify(score: float) -> str:
    from app.config import SIGNAL_LEVELS
    for (low, high), level in SIGNAL_LEVELS.items():
        if low <= score < high:
            return level
    return "CRISIS"
