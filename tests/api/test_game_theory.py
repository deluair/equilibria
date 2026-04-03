"""Tests for /api/game-theory endpoints."""
import pytest


@pytest.mark.asyncio
async def test_game_theory_score_returns_json(async_client):
    """Game theory composite score endpoint returns 200 with JSON payload."""
    resp = await async_client.get("/api/game-theory/score")
    assert resp.status_code == 200
    assert isinstance(resp.json(), dict)


@pytest.mark.asyncio
async def test_game_theory_score_not_500(async_client):
    """Score endpoint should not return 500."""
    resp = await async_client.get("/api/game-theory/score")
    assert resp.status_code != 500


@pytest.mark.asyncio
async def test_game_theory_score_has_layer_key(async_client):
    """Score endpoint response has 'layer' or 'score' key."""
    resp = await async_client.get("/api/game-theory/score")
    assert resp.status_code == 200
    data = resp.json()
    assert "layer" in data or "score" in data
