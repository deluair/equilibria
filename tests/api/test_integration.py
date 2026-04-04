"""Tests for /api/integration endpoints (L6 Integration)."""

import pytest


@pytest.mark.asyncio
async def test_composite_returns_200(async_client):
    """Composite score endpoint returns 200 (real implementation, not stub)."""
    resp = await async_client.get("/api/integration/composite")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_composite_response_structure(async_client):
    """Composite score response has expected keys."""
    resp = await async_client.get("/api/integration/composite")
    body = resp.json()
    assert "signal" in body
    assert "layers" in body
    assert "data_coverage" in body
    assert "methodology" in body


@pytest.mark.asyncio
async def test_composite_layers_have_correct_keys(async_client):
    """Each layer in composite response has name, score, signal, modules."""
    resp = await async_client.get("/api/integration/composite")
    body = resp.json()
    for layer_key, layer_info in body["layers"].items():
        assert "name" in layer_info, f"Layer {layer_key} missing 'name'"
        assert "signal" in layer_info, f"Layer {layer_key} missing 'signal'"
        assert "modules" in layer_info, f"Layer {layer_key} missing 'modules'"


@pytest.mark.asyncio
async def test_composite_data_coverage_keys(async_client):
    """Data coverage dict has expected keys."""
    resp = await async_client.get("/api/integration/composite")
    coverage = resp.json()["data_coverage"]
    assert "total_series" in coverage
    assert "total_data_points" in coverage
    assert "sources" in coverage


@pytest.mark.asyncio
async def test_attribution_returns_json(async_client):
    """Attribution endpoint returns 200 with JSON payload."""
    resp = await async_client.get("/api/integration/attribution")
    assert resp.status_code == 200
    assert isinstance(resp.json(), dict)


@pytest.mark.asyncio
async def test_crisis_comparison_returns_json(async_client):
    """Crisis comparison endpoint returns 200 with JSON payload."""
    resp = await async_client.get("/api/integration/crisis-comparison")
    assert resp.status_code == 200
    assert isinstance(resp.json(), dict)


@pytest.mark.asyncio
async def test_country_profile_returns_json(async_client):
    """Country profile endpoint returns 200 with JSON payload."""
    resp = await async_client.get("/api/integration/country/BGD")
    assert resp.status_code == 200
    assert isinstance(resp.json(), dict)


@pytest.mark.asyncio
async def test_integration_endpoints_not_500(async_client):
    """No integration endpoint should return a 500 server error."""
    endpoints = [
        "/api/integration/composite",
        "/api/integration/attribution",
        "/api/integration/crisis-comparison",
        "/api/integration/country/USA",
    ]
    for url in endpoints:
        resp = await async_client.get(url)
        assert resp.status_code != 500, f"{url} returned 500"
