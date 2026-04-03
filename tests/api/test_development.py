"""Tests for /api/development endpoints (L4 Development)."""

import pytest


@pytest.mark.asyncio
async def test_convergence_returns_501(async_client):
    """Convergence analysis endpoint returns 501."""
    resp = await async_client.get("/api/development/convergence")
    assert resp.status_code == 501


@pytest.mark.asyncio
async def test_poverty_returns_501(async_client):
    """Poverty analysis endpoint returns 501."""
    resp = await async_client.get("/api/development/poverty/BGD")
    assert resp.status_code == 501


@pytest.mark.asyncio
async def test_institutions_returns_501(async_client):
    """Institutional quality endpoint returns 501."""
    resp = await async_client.get("/api/development/institutions/IND")
    assert resp.status_code == 501


@pytest.mark.asyncio
async def test_hdi_returns_501(async_client):
    """HDI decomposition endpoint returns 501."""
    resp = await async_client.get("/api/development/hdi/NOR")
    assert resp.status_code == 501


@pytest.mark.asyncio
async def test_development_score_returns_501(async_client):
    """Development composite score endpoint returns 501."""
    resp = await async_client.get("/api/development/score")
    assert resp.status_code == 501


@pytest.mark.asyncio
async def test_development_endpoints_not_500(async_client):
    """No development endpoint should return a 500 server error."""
    endpoints = [
        "/api/development/convergence",
        "/api/development/poverty/USA",
        "/api/development/institutions/USA",
        "/api/development/hdi/USA",
        "/api/development/score",
    ]
    for url in endpoints:
        resp = await async_client.get(url)
        assert resp.status_code != 500, f"{url} returned 500"
