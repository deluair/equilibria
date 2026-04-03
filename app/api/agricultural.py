"""L5 Agricultural API routes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Response

from app.db import get_db, release_db
from app.layers.agricultural import ALL_MODULES
from app.layers.agricultural.climate_yield import ClimateYield
from app.layers.agricultural.food_security import FoodSecurityIndex
from app.layers.agricultural.price_transmission import PriceTransmission
from app.layers.agricultural.supply_elasticity import SupplyElasticity

router = APIRouter(prefix="/agricultural", tags=["agricultural"])

CACHE_1H = "public, max-age=3600, s-maxage=86400"


@router.get("/food-security/{iso3}")
async def food_security_index(
    iso3: str,
    response: Response,
) -> dict[str, Any]:
    """Composite food security index for a country."""
    response.headers["Cache-Control"] = CACHE_1H
    iso3 = iso3.upper()
    db = await get_db()
    try:
        result = await FoodSecurityIndex().run(db, country_iso3=iso3)
        return result
    finally:
        await release_db(db)


@router.get("/price-transmission")
async def price_transmission(
    response: Response,
) -> dict[str, Any]:
    """Commodity price transmission analysis (VECM, threshold cointegration)."""
    response.headers["Cache-Control"] = CACHE_1H
    db = await get_db()
    try:
        result = await PriceTransmission().run(db)
        return result
    finally:
        await release_db(db)


@router.get("/supply-elasticity")
async def supply_elasticity(
    response: Response,
) -> dict[str, Any]:
    """Agricultural supply elasticity estimation (Nerlove model)."""
    response.headers["Cache-Control"] = CACHE_1H
    db = await get_db()
    try:
        result = await SupplyElasticity().run(db)
        return result
    finally:
        await release_db(db)


@router.get("/climate-yield")
async def climate_yield(
    response: Response,
) -> dict[str, Any]:
    """Climate-yield relationship (panel with weather shocks)."""
    response.headers["Cache-Control"] = CACHE_1H
    db = await get_db()
    try:
        result = await ClimateYield().run(db)
        return result
    finally:
        await release_db(db)


@router.get("/score")
async def agricultural_composite_score(
    response: Response,
) -> dict[str, Any]:
    """Layer 5 composite score across all agricultural indicators."""
    response.headers["Cache-Control"] = CACHE_1H
    db = await get_db()
    try:
        results = []
        for ModuleClass in ALL_MODULES:
            module = ModuleClass()
            r = await module.run(db)
            results.append(r)

        scores = [r["score"] for r in results if r.get("score") is not None]
        avg_score = round(sum(scores) / len(scores), 2) if scores else None

        return {
            "layer": "agricultural",
            "score": avg_score,
            "modules_run": len(results),
            "modules_scored": len(scores),
            "results": results,
        }
    finally:
        await release_db(db)
