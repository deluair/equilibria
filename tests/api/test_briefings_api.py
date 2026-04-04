"""Tests for /api/briefings endpoints."""

import pytest


@pytest.mark.asyncio
async def test_list_briefings_returns_200(async_client):
    """List briefings endpoint returns 200 with JSON payload."""
    resp = await async_client.get("/api/briefings")
    assert resp.status_code == 200
    assert isinstance(resp.json(), dict)


@pytest.mark.asyncio
async def test_list_briefings_with_pagination_params(async_client):
    """List briefings endpoint accepts pagination query params without 500."""
    resp = await async_client.get("/api/briefings?offset=0&limit=10")
    assert resp.status_code != 500


@pytest.mark.asyncio
async def test_list_briefings_invalid_pagination(async_client):
    """Invalid pagination params (limit=0) return 422."""
    resp = await async_client.get("/api/briefings?limit=0")
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_get_briefing_returns_json(async_client):
    """Get briefing by ID returns 200 or 404 (none in test DB)."""
    resp = await async_client.get("/api/briefings/1")
    assert resp.status_code in (200, 404)
    assert isinstance(resp.json(), dict)


@pytest.mark.asyncio
async def test_generate_briefing_returns_json(async_client):
    """Generate briefing endpoint returns 200 or 503 (no API key)."""
    resp = await async_client.post(
        "/api/briefings/generate",
        json={"type": "economic_conditions"},
    )
    assert resp.status_code in (200, 503)


@pytest.mark.asyncio
async def test_generate_briefing_with_country(async_client):
    """Generate briefing with country returns a non-501 status."""
    resp = await async_client.post(
        "/api/briefings/generate",
        json={"type": "country_deep_dive", "params": {"country_iso3": "USA"}},
    )
    assert resp.status_code != 501


@pytest.mark.asyncio
async def test_generate_briefing_missing_body(async_client):
    """Generate briefing without body returns 422."""
    resp = await async_client.post("/api/briefings/generate", json={})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_briefings_endpoints_not_500(async_client):
    """No briefings endpoint should return 500."""
    resp = await async_client.get("/api/briefings")
    assert resp.status_code != 500
    resp = await async_client.get("/api/briefings/999")
    assert resp.status_code != 500
