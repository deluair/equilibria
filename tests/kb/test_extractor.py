"""Tests for Stage 1: fact extraction."""

import json

import pytest

from app.db import fetch_all, fetch_one
from app.kb.extractor import extract_facts_from_result, store_fact


@pytest.mark.asyncio
async def test_store_fact(tmp_db):
    fact_id = await store_fact(
        claim="BGD GDP growth is 6.5%",
        topic="macro",
        subtopic="gdp_decomposition",
        country_iso3="BGD",
        confidence=0.85,
        evidence=[{"type": "analysis_result", "id": 1, "summary": "GDP decomp"}],
        source_type="analysis_result",
        source_id=1,
    )
    assert fact_id > 0
    fact = await fetch_one("SELECT * FROM kb_facts WHERE id = ?", (fact_id,))
    assert fact["claim"] == "BGD GDP growth is 6.5%"
    assert fact["confidence"] == 0.85
    source = await fetch_one("SELECT * FROM kb_sources WHERE fact_id = ?", (fact_id,))
    assert source["source_type"] == "analysis_result"
    assert source["source_id"] == 1


@pytest.mark.asyncio
async def test_store_fact_indexes_in_fts(tmp_db):
    await store_fact(
        claim="Textile exports grew 12%",
        topic="trade",
        confidence=0.8,
        evidence=[],
        source_type="briefing",
        source_id=1,
    )
    fts = await fetch_all("SELECT * FROM kb_search WHERE kb_search MATCH 'textile'")
    assert len(fts) >= 1


@pytest.mark.asyncio
async def test_extract_facts_from_result_dict(tmp_db):
    result = {
        "analysis_type": "rca",
        "country_iso3": "BGD",
        "layer": "l1",
        "result": json.dumps({"rca": 12.3, "product": "textiles"}),
        "score": 75.0,
        "id": 1,
        "created_at": "2026-04-04 12:00:00",
    }
    facts = await extract_facts_from_result(result)
    assert isinstance(facts, list)
