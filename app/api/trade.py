"""L1 Trade API routes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Response

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
    reporter = reporter.upper()
    # TODO: wire to app.layers.trade.gravity
    raise HTTPException(status_code=501, detail="Gravity estimation not yet implemented")


@router.get("/rca/{iso3}")
async def revealed_comparative_advantage(
    iso3: str,
    response: Response,
    year: int = Query(None, description="Year (latest if omitted)"),
) -> dict[str, Any]:
    """Compute Revealed Comparative Advantage for a country."""
    response.headers["Cache-Control"] = CACHE_1H
    iso3 = iso3.upper()
    raise HTTPException(status_code=501, detail="RCA computation not yet implemented")


@router.get("/concentration/{iso3}")
async def hhi_concentration(
    iso3: str,
    response: Response,
) -> dict[str, Any]:
    """Compute HHI export/import concentration for a country."""
    response.headers["Cache-Control"] = CACHE_1H
    iso3 = iso3.upper()
    raise HTTPException(status_code=501, detail="HHI concentration not yet implemented")


@router.get("/openness/{iso3}")
async def trade_openness(
    iso3: str,
    response: Response,
) -> dict[str, Any]:
    """Compute trade openness metrics for a country."""
    response.headers["Cache-Control"] = CACHE_1H
    iso3 = iso3.upper()
    raise HTTPException(status_code=501, detail="Trade openness not yet implemented")


@router.get("/bilateral/{exporter}/{importer}")
async def bilateral_decomposition(
    exporter: str,
    importer: str,
    response: Response,
) -> dict[str, Any]:
    """Bilateral trade decomposition (extensive/intensive margins)."""
    response.headers["Cache-Control"] = CACHE_1H
    exporter = exporter.upper()
    importer = importer.upper()
    raise HTTPException(status_code=501, detail="Bilateral decomposition not yet implemented")


@router.get("/terms-of-trade/{iso3}")
async def terms_of_trade(
    iso3: str,
    response: Response,
) -> dict[str, Any]:
    """Compute terms of trade for a country."""
    response.headers["Cache-Control"] = CACHE_1H
    iso3 = iso3.upper()
    raise HTTPException(status_code=501, detail="Terms of trade not yet implemented")


@router.get("/score")
async def trade_composite_score(
    response: Response,
) -> dict[str, Any]:
    """Layer 1 composite score across all trade indicators."""
    response.headers["Cache-Control"] = CACHE_1H
    raise HTTPException(status_code=501, detail="Trade composite score not yet implemented")
