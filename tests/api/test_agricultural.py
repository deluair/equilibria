"""Tests for /api/agricultural endpoints (L5 Agricultural)."""

import pytest


@pytest.mark.asyncio
async def test_food_security_returns_json(async_client):
    """Food security index endpoint returns 200 with JSON payload."""
    resp = await async_client.get("/api/agricultural/food-security/ETH")
    assert resp.status_code == 200
    assert isinstance(resp.json(), dict)


@pytest.mark.asyncio
async def test_price_transmission_returns_json(async_client):
    """Price transmission endpoint returns 200 with JSON payload."""
    resp = await async_client.get("/api/agricultural/price-transmission")
    assert resp.status_code == 200
    assert isinstance(resp.json(), dict)


@pytest.mark.asyncio
async def test_supply_elasticity_returns_json(async_client):
    """Supply elasticity endpoint returns 200 with JSON payload."""
    resp = await async_client.get("/api/agricultural/supply-elasticity")
    assert resp.status_code == 200
    assert isinstance(resp.json(), dict)


@pytest.mark.asyncio
async def test_climate_yield_returns_json(async_client):
    """Climate yield endpoint returns 200 with JSON payload."""
    resp = await async_client.get("/api/agricultural/climate-yield")
    assert resp.status_code == 200
    assert isinstance(resp.json(), dict)


@pytest.mark.asyncio
async def test_agricultural_score_returns_json(async_client):
    """Agricultural composite score endpoint returns 200 with JSON payload."""
    resp = await async_client.get("/api/agricultural/score")
    assert resp.status_code == 200
    assert isinstance(resp.json(), dict)


@pytest.mark.asyncio
async def test_agricultural_endpoints_not_500(async_client):
    """No agricultural endpoint should return a 500 server error."""
    endpoints = [
        "/api/agricultural/food-security/USA",
        "/api/agricultural/price-transmission",
        "/api/agricultural/supply-elasticity",
        "/api/agricultural/climate-yield",
        "/api/agricultural/score",
    ]
    for url in endpoints:
        resp = await async_client.get(url)
        assert resp.status_code != 500, f"{url} returned 500"
