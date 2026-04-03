"""Tests for /api/macro endpoints (L2 Macro)."""

import pytest


@pytest.mark.asyncio
async def test_gdp_returns_501(async_client):
    """GDP decomposition endpoint returns 501."""
    resp = await async_client.get("/api/macro/gdp/USA")
    assert resp.status_code == 501


@pytest.mark.asyncio
async def test_phillips_returns_501(async_client):
    """Phillips curve endpoint returns 501."""
    resp = await async_client.get("/api/macro/phillips")
    assert resp.status_code == 501


@pytest.mark.asyncio
async def test_taylor_returns_501(async_client):
    """Taylor rule endpoint returns 501."""
    resp = await async_client.get("/api/macro/taylor")
    assert resp.status_code == 501


@pytest.mark.asyncio
async def test_cycle_returns_501(async_client):
    """Business cycle endpoint returns 501."""
    resp = await async_client.get("/api/macro/cycle")
    assert resp.status_code == 501


@pytest.mark.asyncio
async def test_fci_returns_501(async_client):
    """FCI endpoint returns 501."""
    resp = await async_client.get("/api/macro/fci")
    assert resp.status_code == 501


@pytest.mark.asyncio
async def test_recession_probability_returns_501(async_client):
    """Recession probability endpoint returns 501."""
    resp = await async_client.get("/api/macro/recession-probability")
    assert resp.status_code == 501


@pytest.mark.asyncio
async def test_macro_score_returns_501(async_client):
    """Macro composite score endpoint returns 501."""
    resp = await async_client.get("/api/macro/score")
    assert resp.status_code == 501


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
