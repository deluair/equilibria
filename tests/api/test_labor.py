"""Tests for /api/labor endpoints (L3 Labor)."""

import pytest


@pytest.mark.asyncio
async def test_wages_returns_501(async_client):
    """Wage analysis endpoint returns 501."""
    resp = await async_client.get("/api/labor/wages")
    assert resp.status_code == 501


@pytest.mark.asyncio
async def test_education_returns_501(async_client):
    """Returns to education endpoint returns 501."""
    resp = await async_client.get("/api/labor/education")
    assert resp.status_code == 501


@pytest.mark.asyncio
async def test_tightness_returns_501(async_client):
    """Labor market tightness endpoint returns 501."""
    resp = await async_client.get("/api/labor/tightness")
    assert resp.status_code == 501


@pytest.mark.asyncio
async def test_beveridge_returns_501(async_client):
    """Beveridge curve endpoint returns 501."""
    resp = await async_client.get("/api/labor/beveridge")
    assert resp.status_code == 501


@pytest.mark.asyncio
async def test_labor_score_returns_501(async_client):
    """Labor composite score endpoint returns 501."""
    resp = await async_client.get("/api/labor/score")
    assert resp.status_code == 501


@pytest.mark.asyncio
async def test_labor_endpoints_not_500(async_client):
    """No labor endpoint should return a 500 server error."""
    endpoints = [
        "/api/labor/wages",
        "/api/labor/education",
        "/api/labor/tightness",
        "/api/labor/beveridge",
        "/api/labor/score",
    ]
    for url in endpoints:
        resp = await async_client.get(url)
        assert resp.status_code != 500, f"{url} returned 500"
