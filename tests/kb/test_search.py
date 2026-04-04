"""Tests for knowledge base schema and search."""

import pytest

from app.db import execute, fetch_all, fetch_one


@pytest.mark.asyncio
async def test_kb_tables_exist(tmp_db):
    tables = await fetch_all(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'kb_%'"
    )
    names = {t["name"] for t in tables}
    assert "kb_facts" in names
    assert "kb_articles" in names
    assert "kb_article_facts" in names
    assert "kb_sources" in names


@pytest.mark.asyncio
async def test_kb_fts_table_exists(tmp_db):
    tables = await fetch_all(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = 'kb_search'"
    )
    assert len(tables) == 1


@pytest.mark.asyncio
async def test_insert_and_query_fact(tmp_db):
    await execute(
        "INSERT INTO kb_facts (claim, topic, subtopic, country_iso3, confidence, evidence) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        ("BGD RCA in textiles is 12.3", "trade", "rca", "BGD", 0.9, "[]"),
    )
    row = await fetch_one("SELECT * FROM kb_facts WHERE topic = 'trade'")
    assert row is not None
    assert row["claim"] == "BGD RCA in textiles is 12.3"
    assert row["confidence"] == 0.9


@pytest.mark.asyncio
async def test_insert_and_query_article(tmp_db):
    await execute(
        "INSERT INTO kb_articles (slug, title, topic, content, summary, fact_count) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        ("bgd-trade", "Bangladesh Trade", "trade", "# Content", "Summary", 3),
    )
    row = await fetch_one("SELECT * FROM kb_articles WHERE slug = 'bgd-trade'")
    assert row is not None
    assert row["title"] == "Bangladesh Trade"


from app.kb.search import index_article, index_fact, search_kb


@pytest.mark.asyncio
async def test_index_and_search_fact(tmp_db):
    await execute(
        "INSERT INTO kb_facts (id, claim, topic, confidence, evidence) VALUES (?, ?, ?, ?, ?)",
        (1, "Bangladesh textile RCA is 12.3", "trade", 0.9, "[]"),
    )
    await index_fact(1, "Bangladesh textile RCA is 12.3")
    results = await search_kb("textile RCA")
    assert any(r["fact_id"] == 1 for r in results["facts"])


@pytest.mark.asyncio
async def test_index_and_search_article(tmp_db):
    await execute(
        "INSERT INTO kb_articles (id, slug, title, topic, content, summary, fact_count) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (1, "bgd-trade", "Bangladesh Trade Overview", "trade", "Full content here", "Summary", 5),
    )
    await index_article(1, "Bangladesh Trade Overview", "Full content here")
    results = await search_kb("Bangladesh Trade")
    assert any(r["article_id"] == 1 for r in results["articles"])


@pytest.mark.asyncio
async def test_search_empty_kb(tmp_db):
    results = await search_kb("anything")
    assert results["facts"] == []
    assert results["articles"] == []


@pytest.mark.asyncio
async def test_search_with_topic_filter(tmp_db):
    await execute(
        "INSERT INTO kb_facts (id, claim, topic, confidence, evidence) VALUES (?, ?, ?, ?, ?)",
        (1, "Trade fact", "trade", 0.9, "[]"),
    )
    await execute(
        "INSERT INTO kb_facts (id, claim, topic, confidence, evidence) VALUES (?, ?, ?, ?, ?)",
        (2, "Macro fact about trade", "macro", 0.8, "[]"),
    )
    await index_fact(1, "Trade fact")
    await index_fact(2, "Macro fact about trade")
    results = await search_kb("trade", topic="trade")
    fact_ids = [r["fact_id"] for r in results["facts"]]
    assert 1 in fact_ids
    assert 2 not in fact_ids
