"""Tests for /api/trade endpoints (L1 Trade)."""

import pytest


@pytest.mark.asyncio
async def test_gravity_missing_params(async_client):
    """Gravity endpoint returns 422 when required params are absent."""
    resp = await async_client.get("/api/trade/gravity")
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_gravity_returns_501(async_client):
    """Gravity endpoint returns 501 (not implemented) when called with valid params."""
    resp = await async_client.get("/api/trade/gravity?reporter=USA&year=2022")
    assert resp.status_code == 501


@pytest.mark.asyncio
async def test_rca_returns_501(async_client):
    """RCA endpoint returns 501."""
    resp = await async_client.get("/api/trade/rca/USA")
    assert resp.status_code == 501


@pytest.mark.asyncio
async def test_concentration_returns_501(async_client):
    """Concentration endpoint returns 501."""
    resp = await async_client.get("/api/trade/concentration/DEU")
    assert resp.status_code == 501


@pytest.mark.asyncio
async def test_openness_returns_501(async_client):
    """Trade openness endpoint returns 501."""
    resp = await async_client.get("/api/trade/openness/JPN")
    assert resp.status_code == 501


@pytest.mark.asyncio
async def test_bilateral_returns_501(async_client):
    """Bilateral decomposition endpoint returns 501."""
    resp = await async_client.get("/api/trade/bilateral/USA/CHN")
    assert resp.status_code == 501


@pytest.mark.asyncio
async def test_terms_of_trade_returns_501(async_client):
    """Terms of trade endpoint returns 501."""
    resp = await async_client.get("/api/trade/terms-of-trade/GBR")
    assert resp.status_code == 501


@pytest.mark.asyncio
async def test_trade_score_returns_501(async_client):
    """Trade composite score endpoint returns 501."""
    resp = await async_client.get("/api/trade/score")
    assert resp.status_code == 501


@pytest.mark.asyncio
async def test_trade_endpoints_not_500(async_client):
    """No trade endpoint should return a 500 server error."""
    endpoints = [
        "/api/trade/rca/USA",
        "/api/trade/concentration/USA",
        "/api/trade/openness/USA",
        "/api/trade/bilateral/USA/CHN",
        "/api/trade/terms-of-trade/USA",
        "/api/trade/score",
    ]
    for url in endpoints:
        resp = await async_client.get(url)
        assert resp.status_code != 500, f"{url} returned 500"
