"""Tests for /api/kb endpoints."""
import pytest


@pytest.mark.asyncio
async def test_kb_stats_empty(async_client):
    resp = await async_client.get("/api/kb/stats")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_facts"] == 0
    assert data["total_articles"] == 0


@pytest.mark.asyncio
async def test_kb_articles_empty(async_client):
    resp = await async_client.get("/api/kb/articles")
    assert resp.status_code == 200
    assert resp.json()["articles"] == []


@pytest.mark.asyncio
async def test_kb_facts_empty(async_client):
    resp = await async_client.get("/api/kb/facts")
    assert resp.status_code == 200
    assert resp.json()["facts"] == []


@pytest.mark.asyncio
async def test_kb_search_empty(async_client):
    resp = await async_client.get("/api/kb/search", params={"q": "trade"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["facts"] == []
    assert data["articles"] == []


@pytest.mark.asyncio
async def test_kb_article_not_found(async_client):
    resp = await async_client.get("/api/kb/articles/nonexistent-slug")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_kb_compile_returns_200(async_client):
    resp = await async_client.post("/api/kb/compile")
    assert resp.status_code == 200
