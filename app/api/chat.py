"""AI Chat API routes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/chat", tags=["chat"])


class ChatRequest(BaseModel):
    message: str
    conversation_id: str | None = None


class ChatResponse(BaseModel):
    response: str
    citations: list[dict[str, Any]]
    tool_calls: list[dict[str, Any]]


@router.post("")
async def chat(request: ChatRequest) -> dict[str, Any]:
    """Send a message to the AI brain and get an analysis response."""
    raise HTTPException(status_code=501, detail="AI chat not yet implemented")
