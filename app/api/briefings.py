"""Briefings API routes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Response
from pydantic import BaseModel

from app.briefings.agricultural_outlook import AgriculturalOutlookBriefing
from app.briefings.country_deep_dive import CountryDeepDiveBriefing
from app.briefings.development_tracker import DevelopmentTrackerBriefing
from app.briefings.economic_conditions import EconomicConditionsBriefing
from app.briefings.labor_pulse import LaborPulseBriefing
from app.briefings.policy_alert import PolicyAlertBriefing
from app.briefings.trade_flash import TradeFlashBriefing
from app.db import get_db, release_db

router = APIRouter(prefix="/briefings", tags=["briefings"])

CACHE_1H = "public, max-age=3600, s-maxage=86400"

GENERATORS = {
    "economic_conditions": EconomicConditionsBriefing,
    "trade_flash": TradeFlashBriefing,
    "country_deep_dive": CountryDeepDiveBriefing,
    "labor_pulse": LaborPulseBriefing,
    "development_tracker": DevelopmentTrackerBriefing,
    "agricultural_outlook": AgriculturalOutlookBriefing,
    "policy_alert": PolicyAlertBriefing,
}


class BriefingGenerateRequest(BaseModel):
    type: str
    params: dict[str, Any] = {}


@router.get("")
async def list_briefings(
    response: Response,
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    """List all briefings with pagination."""
    response.headers["Cache-Control"] = CACHE_1H
    db = await get_db()
    try:
        total_row = await db.fetch_one("SELECT COUNT(*) AS n FROM briefings")
        total = total_row["n"] if total_row else 0
        rows = await db.fetch_all(
            "SELECT id, country_iso3, title, composite_score, signal, created_at "
            "FROM briefings ORDER BY created_at DESC LIMIT ? OFFSET ?",
            (limit, offset),
        )
    finally:
        await release_db(db)

    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "items": rows,
    }


@router.get("/{briefing_id}")
async def get_briefing(
    briefing_id: int,
    response: Response,
) -> dict[str, Any]:
    """Get a specific briefing by ID."""
    response.headers["Cache-Control"] = CACHE_1H
    db = await get_db()
    try:
        row = await db.fetch_one(
            "SELECT * FROM briefings WHERE id = ?", (briefing_id,)
        )
    finally:
        await release_db(db)

    if row is None:
        raise HTTPException(status_code=404, detail=f"Briefing {briefing_id} not found")
    return row


@router.post("/generate")
async def generate_briefing(
    request: BriefingGenerateRequest,
) -> dict[str, Any]:
    """Generate a briefing by type (economic_conditions, trade_flash, country_deep_dive, labor_pulse, development_tracker, agricultural_outlook, policy_alert)."""
    generator_cls = GENERATORS.get(request.type)
    if generator_cls is None:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown briefing type '{request.type}'. Valid types: {list(GENERATORS)}",
        )

    params = request.params

    # Instantiate with country_iso3 for country_deep_dive
    if request.type == "country_deep_dive":
        country_iso3 = params.get("country_iso3", "USA")
        generator = generator_cls(country_iso3=country_iso3)
    else:
        generator = generator_cls()

    db = await get_db()
    try:
        result = await generator.generate(db, **params)
        briefing_id = await generator.save(result, db)
    finally:
        await release_db(db)

    result["id"] = briefing_id
    return result
