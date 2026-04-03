"""Briefings API routes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Response
from pydantic import BaseModel

router = APIRouter(prefix="/briefings", tags=["briefings"])

CACHE_1H = "public, max-age=3600, s-maxage=86400"


class BriefingGenerateRequest(BaseModel):
    briefing_type: str
    country_iso3: str | None = None


@router.get("")
async def list_briefings(
    response: Response,
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    """List all briefings with pagination."""
    response.headers["Cache-Control"] = CACHE_1H
    raise HTTPException(status_code=501, detail="List briefings not yet implemented")


@router.get("/{briefing_id}")
async def get_briefing(
    briefing_id: int,
    response: Response,
) -> dict[str, Any]:
    """Get a specific briefing by ID."""
    response.headers["Cache-Control"] = CACHE_1H
    raise HTTPException(status_code=501, detail="Get briefing not yet implemented")


@router.post("/generate")
async def generate_briefing(
    request: BriefingGenerateRequest,
) -> dict[str, Any]:
    """Generate a briefing by type (economic_conditions, trade_flash, etc.)."""
    raise HTTPException(status_code=501, detail="Generate briefing not yet implemented")
