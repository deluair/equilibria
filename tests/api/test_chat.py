"""Tests for /api/chat endpoint."""

import pytest


@pytest.mark.asyncio
async def test_chat_returns_501(async_client):
    """Chat endpoint returns 501 (not yet implemented)."""
    resp = await async_client.post(
        "/api/chat",
        json={"message": "What is the current GDP growth rate?"},
    )
    assert resp.status_code == 501


@pytest.mark.asyncio
async def test_chat_with_conversation_id(async_client):
    """Chat endpoint with conversation_id returns 501, not 422 or 500."""
    resp = await async_client.post(
        "/api/chat",
        json={"message": "Analyze trade balance", "conversation_id": "abc-123"},
    )
    assert resp.status_code == 501


@pytest.mark.asyncio
async def test_chat_missing_message_returns_422(async_client):
    """Chat endpoint requires message field - returns 422 if absent."""
    resp = await async_client.post("/api/chat", json={})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_chat_not_500(async_client):
    """Chat endpoint must not return 500."""
    resp = await async_client.post(
        "/api/chat",
        json={"message": "hello"},
    )
    assert resp.status_code != 500
