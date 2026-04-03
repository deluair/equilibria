"""Tests for /api/macro endpoints (L2 Macro)."""

import pytest


@pytest.mark.asyncio
async def test_gdp_returns_json(async_client):
    """GDP decomposition endpoint returns 200 with JSON payload."""
    resp = await async_client.get("/api/macro/gdp/USA")
    assert resp.status_code == 200
    assert isinstance(resp.json(), dict)


@pytest.mark.asyncio
async def test_phillips_returns_json(async_client):
    """Phillips curve endpoint returns 200 with JSON payload."""
    resp = await async_client.get("/api/macro/phillips")
    assert resp.status_code == 200
    assert isinstance(resp.json(), dict)


@pytest.mark.asyncio
async def test_taylor_returns_json(async_client):
    """Taylor rule endpoint returns 200 with JSON payload."""
    resp = await async_client.get("/api/macro/taylor")
    assert resp.status_code == 200
    assert isinstance(resp.json(), dict)


@pytest.mark.asyncio
async def test_cycle_returns_json(async_client):
    """Business cycle endpoint returns 200 with JSON payload."""
    resp = await async_client.get("/api/macro/cycle")
    assert resp.status_code == 200
    assert isinstance(resp.json(), dict)


@pytest.mark.asyncio
async def test_fci_returns_json(async_client):
    """FCI endpoint returns 200 with JSON payload."""
    resp = await async_client.get("/api/macro/fci")
    assert resp.status_code == 200
    assert isinstance(resp.json(), dict)


@pytest.mark.asyncio
async def test_recession_probability_returns_json(async_client):
    """Recession probability endpoint returns 200 with JSON payload."""
    resp = await async_client.get("/api/macro/recession-probability")
    assert resp.status_code == 200
    assert isinstance(resp.json(), dict)


@pytest.mark.asyncio
async def test_macro_score_returns_json(async_client):
    """Macro composite score endpoint returns 200 with JSON payload."""
    resp = await async_client.get("/api/macro/score")
    assert resp.status_code == 200
    assert isinstance(resp.json(), dict)


@pytest.mark.asyncio
async def test_macro_endpoints_not_500(async_client):
    """No macro endpoint should return a 500 server error."""
    endpoints = [
        "/api/macro/gdp/DEU",
        "/api/macro/phillips",
        "/api/macro/taylor",
        "/api/macro/cycle",
        "/api/macro/fci",
        "/api/macro/recession-probability",
        "/api/macro/score",
    ]
    for url in endpoints:
        resp = await async_client.get(url)
        assert resp.status_code != 500, f"{url} returned 500"
