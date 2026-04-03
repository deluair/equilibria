"""Tests for /api/health endpoints."""

import pytest


@pytest.mark.asyncio
async def test_root_health_200(async_client):
    """Root health check returns 200 with status ok."""
    resp = await async_client.get("/api/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"


@pytest.mark.asyncio
async def test_root_health_fields(async_client):
    """Root health check includes app name and version."""
    resp = await async_client.get("/api/health")
    body = resp.json()
    assert "app" in body
    assert "version" in body


@pytest.mark.asyncio
async def test_api_health_router_200(async_client):
    """Router-level /api/health returns 200."""
    resp = await async_client.get("/api/health")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_api_health_router_fields(async_client):
    """Router health endpoint returns expected structural fields."""
    resp = await async_client.get("/api/health")
    body = resp.json()
    assert "status" in body
    assert body["status"] == "ok"


@pytest.mark.asyncio
async def test_custom_headers_present(async_client):
    """Custom X-Crafted-By and X-Origin headers are present on responses."""
    resp = await async_client.get("/api/health")
    assert resp.headers.get("x-crafted-by") == "Md Deluair Hossen, PhD"
    assert resp.headers.get("x-origin") == "equilibria"
