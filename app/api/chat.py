"""AI Chat API routes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.ai.brain import analyze
from app.config import settings

router = APIRouter(prefix="/chat", tags=["chat"])


class ChatRequest(BaseModel):
    message: str
    conversation_id: str | None = None


class ChatResponse(BaseModel):
    response: str
    citations: list[dict[str, Any]]
    tool_calls: list[dict[str, Any]]
    token_usage: dict[str, Any] | None = None


@router.post("")
async def chat(request: ChatRequest) -> ChatResponse:
    """Send a message to the AI brain and get an analysis response."""
    if not settings.anthropic_api_key:
        raise HTTPException(
            status_code=503,
            detail="ANTHROPIC_API_KEY is not configured. Set it in your .env file.",
        )
    result = await analyze(
        question=request.message,
        conversation_history=None,  # TODO: load from conversation_id
    )
    return ChatResponse(**result)
