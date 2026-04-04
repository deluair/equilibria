"""Tests for KB brain tools."""

import pytest

from app.ai.tools import TOOL_REGISTRY, execute_tool


@pytest.mark.asyncio
async def test_search_knowledge_tool_exists():
    assert "search_knowledge" in TOOL_REGISTRY


@pytest.mark.asyncio
async def test_file_insight_tool_exists():
    assert "file_insight" in TOOL_REGISTRY


@pytest.mark.asyncio
async def test_search_knowledge_empty(tmp_db):
    result = await execute_tool("search_knowledge", {"query": "trade"})
    assert result["facts"] == []
    assert result["articles"] == []


@pytest.mark.asyncio
async def test_file_insight_creates_fact(tmp_db):
    result = await execute_tool("file_insight", {
        "claim": "BGD textile exports grew 12%",
        "topic": "trade",
        "subtopic": "rca",
        "country_iso3": "BGD",
        "evidence": [{"type": "analysis_result", "id": 1, "summary": "RCA analysis"}],
    })
    assert result["status"] == "created"
    assert result["fact_id"] > 0


@pytest.mark.asyncio
async def test_search_knowledge_finds_filed_insight(tmp_db):
    await execute_tool("file_insight", {
        "claim": "Bangladesh has strong textile comparative advantage",
        "topic": "trade",
        "evidence": [{"type": "analysis_result", "id": 1, "summary": "RCA"}],
    })
    result = await execute_tool("search_knowledge", {"query": "textile comparative advantage"})
    assert len(result["facts"]) >= 1
