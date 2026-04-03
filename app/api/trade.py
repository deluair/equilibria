"""L1 Trade API routes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query, Response

from app.db import get_db, release_db
from app.layers.trade.bilateral_decomposition import BilateralDecomposition
from app.layers.trade.concentration import TradeConcentration
from app.layers.trade.gravity import GravityModel
from app.layers.trade.rca import RevealedComparativeAdvantage
from app.layers.trade.terms_of_trade import TermsOfTrade
from app.layers.trade.trade_openness import TradeOpenness
from app.layers.trade import ALL_MODULES

router = APIRouter(prefix="/trade", tags=["trade"])

CACHE_1H = "public, max-age=3600, s-maxage=86400"


@router.get("/gravity")
async def gravity_estimation(
    response: Response,
    reporter: str = Query(..., description="Reporter ISO3 code"),
    year: int = Query(..., description="Year"),
) -> dict[str, Any]:
    """Run gravity model estimation for a reporter-year pair."""
    response.headers["Cache-Control"] = CACHE_1H
    db = await get_db()
    try:
        result = await GravityModel().run(db, country_iso3=reporter.upper(), year=year)
        return result
    finally:
        await release_db(db)


@router.get("/rca/{iso3}")
async def revealed_comparative_advantage(
    iso3: str,
    response: Response,
    year: int = Query(None, description="Year (latest if omitted)"),
) -> dict[str, Any]:
    """Compute Revealed Comparative Advantage for a country."""
    response.headers["Cache-Control"] = CACHE_1H
    db = await get_db()
    try:
        result = await RevealedComparativeAdvantage().run(db, country_iso3=iso3.upper(), year=year)
        return result
    finally:
        await release_db(db)


@router.get("/concentration/{iso3}")
async def hhi_concentration(
    iso3: str,
    response: Response,
) -> dict[str, Any]:
    """Compute HHI export/import concentration for a country."""
    response.headers["Cache-Control"] = CACHE_1H
    db = await get_db()
    try:
        result = await TradeConcentration().run(db, country_iso3=iso3.upper())
        return result
    finally:
        await release_db(db)


@router.get("/openness/{iso3}")
async def trade_openness(
    iso3: str,
    response: Response,
) -> dict[str, Any]:
    """Compute trade openness metrics for a country."""
    response.headers["Cache-Control"] = CACHE_1H
    db = await get_db()
    try:
        result = await TradeOpenness().run(db, country_iso3=iso3.upper())
        return result
    finally:
        await release_db(db)


@router.get("/bilateral/{exporter}/{importer}")
async def bilateral_decomposition(
    exporter: str,
    importer: str,
    response: Response,
) -> dict[str, Any]:
    """Bilateral trade decomposition (extensive/intensive margins)."""
    response.headers["Cache-Control"] = CACHE_1H
    db = await get_db()
    try:
        result = await BilateralDecomposition().run(
            db, country_iso3=exporter.upper(), partner_iso3=importer.upper()
        )
        return result
    finally:
        await release_db(db)


@router.get("/terms-of-trade/{iso3}")
async def terms_of_trade(
    iso3: str,
    response: Response,
) -> dict[str, Any]:
    """Compute terms of trade for a country."""
    response.headers["Cache-Control"] = CACHE_1H
    db = await get_db()
    try:
        result = await TermsOfTrade().run(db, country_iso3=iso3.upper())
        return result
    finally:
        await release_db(db)


@router.get("/score")
async def trade_composite_score(
    response: Response,
) -> dict[str, Any]:
    """Layer 1 composite score across all trade indicators."""
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
            "layer": "trade",
            "score": avg_score,
            "modules_run": len(results),
            "modules_scored": len(scores),
            "results": results,
        }
    finally:
        await release_db(db)
