"""AI Chat API routes."""

from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.ai.brain import analyze
from app.config import settings
from app.db import execute, fetch_all

router = APIRouter(prefix="/chat", tags=["chat"])


class ChatRequest(BaseModel):
    message: str
    conversation_id: str | None = None


class ChatResponse(BaseModel):
    response: str
    citations: list[dict[str, Any]]
    tool_calls: list[dict[str, Any]]
    token_usage: dict[str, Any] | None = None
    conversation_id: str


async def _load_history(conversation_id: str) -> list[dict[str, str]] | None:
    rows = await fetch_all(
        "SELECT role, content FROM conversation_messages WHERE conversation_id = ? ORDER BY id",
        (conversation_id,),
    )
    return [{"role": r["role"], "content": r["content"]} for r in rows] if rows else None


async def _save_message(conversation_id: str, role: str, content: str):
    await execute(
        "INSERT OR IGNORE INTO conversations (id) VALUES (?)",
        (conversation_id,),
    )
    await execute(
        "INSERT INTO conversation_messages (conversation_id, role, content) VALUES (?, ?, ?)",
        (conversation_id, role, content),
    )


@router.post("")
async def chat(request: ChatRequest) -> ChatResponse:
    """Send a message to the AI brain and get an analysis response."""
    if not settings.anthropic_api_key:
        raise HTTPException(
            status_code=503,
            detail="ANTHROPIC_API_KEY is not configured. Set it in your .env file.",
        )
    conv_id = request.conversation_id or str(uuid.uuid4())
    history = await _load_history(conv_id) if request.conversation_id else None

    result = await analyze(
        question=request.message,
        conversation_history=history,
    )

    await _save_message(conv_id, "user", request.message)
    await _save_message(conv_id, "assistant", result["response"])

    return ChatResponse(**result, conversation_id=conv_id)
