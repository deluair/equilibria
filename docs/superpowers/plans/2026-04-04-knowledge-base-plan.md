# Knowledge Base Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an LLM-compiled knowledge base to Equilibria that auto-accumulates insights from analysis runs, briefings, and conversations, then exposes them via AI tools and a frontend wiki UI.

**Architecture:** Four new DB tables (kb_facts, kb_articles, kb_article_facts, kb_sources) + FTS5 virtual table in equilibria.db. Three-stage async compiler pipeline (extract, compile, sweep). Two new AI brain tools. Six REST endpoints. Three frontend pages. Zero new dependencies.

**Tech Stack:** Python 3.11, aiosqlite, FastAPI, Anthropic SDK, Next.js 16, React 19, Recharts, Tailwind 4

---

## File Structure

### New Files
```
app/kb/__init__.py           — Package init
app/kb/compiler.py           — Orchestrator: compile_kb()
app/kb/extractor.py          — Stage 1: fact extraction from sources
app/kb/articler.py           — Stage 2: article compilation from facts
app/kb/staleness.py          — Stage 3: staleness sweep
app/kb/search.py             — FTS5 search helpers
app/api/kb.py                — 6 REST endpoints
tests/kb/__init__.py         — Test package
tests/kb/test_search.py      — FTS5 search tests
tests/kb/test_extractor.py   — Fact extraction tests
tests/kb/test_articler.py    — Article compilation tests
tests/kb/test_staleness.py   — Staleness sweep tests
tests/api/test_kb.py         — API endpoint tests
web/app/knowledge/page.tsx   — KB index page
web/app/knowledge/[slug]/page.tsx — Article detail page
web/app/knowledge/facts/page.tsx  — Fact explorer page
```

### Modified Files
```
app/db.py                    — Add 4 tables + FTS5 + indexes
app/ai/tools.py              — Add search_knowledge + file_insight tools
app/ai/brain.py              — Append KB instructions to system prompt
app/main.py                  — Register kb router
app/collectors/base.py       — Post-collect hook
app/briefings/base.py        — Post-generate hook
web/app/Sidebar.tsx          — Add KB nav entry
web/app/page.tsx             — Add KB stats card
```

---

### Task 1: Database Schema — KB Tables

**Files:**
- Modify: `app/db.py`
- Create: `app/kb/__init__.py`
- Test: `tests/kb/__init__.py`

- [ ] **Step 1: Write failing test for KB tables**

Create `tests/kb/__init__.py`:
```python
```

Create `tests/kb/test_search.py`:
```python
"""Tests for knowledge base schema and search."""

import pytest

from app.db import execute, fetch_all, fetch_one


@pytest.mark.asyncio
async def test_kb_tables_exist(tmp_db):
    """KB tables are created on init_db."""
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
    """FTS5 virtual table kb_search is created."""
    tables = await fetch_all(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = 'kb_search'"
    )
    assert len(tables) == 1


@pytest.mark.asyncio
async def test_insert_and_query_fact(tmp_db):
    """Can insert a fact and retrieve it."""
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
    """Can insert an article and retrieve by slug."""
    await execute(
        "INSERT INTO kb_articles (slug, title, topic, content, summary, fact_count) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        ("bgd-trade", "Bangladesh Trade", "trade", "# Content", "Summary", 3),
    )
    row = await fetch_one("SELECT * FROM kb_articles WHERE slug = 'bgd-trade'")
    assert row is not None
    assert row["title"] == "Bangladesh Trade"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/mddeluairhossen/equilibria && uv run pytest tests/kb/test_search.py -v`
Expected: FAIL with "no such table: kb_facts"

- [ ] **Step 3: Add KB tables to SCHEMA in db.py**

In `app/db.py`, append the following to the SCHEMA string, after the `idx_conv_messages_conv` index:

```python
CREATE TABLE IF NOT EXISTS kb_facts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    claim TEXT NOT NULL,
    topic TEXT NOT NULL,
    subtopic TEXT,
    country_iso3 TEXT,
    confidence REAL DEFAULT 0.5,
    evidence TEXT DEFAULT '[]',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    stale_at TEXT DEFAULT (datetime('now', '+30 days')),
    is_stale INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS kb_articles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    slug TEXT UNIQUE NOT NULL,
    title TEXT NOT NULL,
    topic TEXT NOT NULL,
    country_iso3 TEXT,
    content TEXT NOT NULL,
    summary TEXT NOT NULL,
    fact_count INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS kb_article_facts (
    article_id INTEGER NOT NULL REFERENCES kb_articles(id),
    fact_id INTEGER NOT NULL REFERENCES kb_facts(id),
    PRIMARY KEY (article_id, fact_id)
);

CREATE TABLE IF NOT EXISTS kb_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fact_id INTEGER NOT NULL REFERENCES kb_facts(id),
    source_type TEXT NOT NULL,
    source_id INTEGER NOT NULL,
    source_date TEXT NOT NULL
);

CREATE VIRTUAL TABLE IF NOT EXISTS kb_search USING fts5(
    fact_id,
    article_id,
    title,
    content,
    content_rowid=rowid
);

CREATE INDEX IF NOT EXISTS idx_kb_facts_topic ON kb_facts(topic, country_iso3);
CREATE INDEX IF NOT EXISTS idx_kb_facts_stale ON kb_facts(is_stale);
CREATE INDEX IF NOT EXISTS idx_kb_facts_subtopic ON kb_facts(subtopic);
CREATE INDEX IF NOT EXISTS idx_kb_articles_topic ON kb_articles(topic, country_iso3);
CREATE INDEX IF NOT EXISTS idx_kb_sources_type ON kb_sources(source_type, source_id);
CREATE INDEX IF NOT EXISTS idx_kb_sources_fact ON kb_sources(fact_id);
```

Also create `app/kb/__init__.py`:
```python
"""Equilibria Knowledge Base — LLM-compiled economics wiki."""
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/mddeluairhossen/equilibria && uv run pytest tests/kb/test_search.py -v`
Expected: All 4 tests PASS

- [ ] **Step 5: Commit**

```bash
git add app/db.py app/kb/__init__.py tests/kb/__init__.py tests/kb/test_search.py
git commit -m "add knowledge base schema: 4 tables, FTS5, indexes"
```

---

### Task 2: FTS5 Search Helpers

**Files:**
- Create: `app/kb/search.py`
- Modify: `tests/kb/test_search.py`

- [ ] **Step 1: Write failing tests for search**

Append to `tests/kb/test_search.py`:

```python
from app.kb.search import index_fact, index_article, search_kb


@pytest.mark.asyncio
async def test_index_and_search_fact(tmp_db):
    """Index a fact and find it via FTS5."""
    await execute(
        "INSERT INTO kb_facts (id, claim, topic, confidence, evidence) VALUES (?, ?, ?, ?, ?)",
        (1, "Bangladesh textile RCA is 12.3", "trade", 0.9, "[]"),
    )
    await index_fact(1, "Bangladesh textile RCA is 12.3")
    results = await search_kb("textile RCA")
    assert any(r["fact_id"] == 1 for r in results["facts"])


@pytest.mark.asyncio
async def test_index_and_search_article(tmp_db):
    """Index an article and find it via FTS5."""
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
    """Search on empty KB returns empty results."""
    results = await search_kb("anything")
    assert results["facts"] == []
    assert results["articles"] == []


@pytest.mark.asyncio
async def test_search_with_topic_filter(tmp_db):
    """Search filters by topic."""
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/mddeluairhossen/equilibria && uv run pytest tests/kb/test_search.py::test_index_and_search_fact -v`
Expected: FAIL with "cannot import name 'index_fact' from 'app.kb.search'"

- [ ] **Step 3: Implement search.py**

Create `app/kb/search.py`:

```python
"""FTS5 search helpers for the knowledge base."""

from __future__ import annotations

import logging

from app.db import execute, fetch_all

logger = logging.getLogger(__name__)


async def index_fact(fact_id: int, claim: str) -> None:
    """Insert or replace a fact in the FTS5 index."""
    await execute(
        "INSERT OR REPLACE INTO kb_search (rowid, fact_id, article_id, title, content) "
        "VALUES (?, ?, ?, ?, ?)",
        (fact_id, str(fact_id), "", "", claim),
    )


async def index_article(article_id: int, title: str, content: str) -> None:
    """Insert or replace an article in the FTS5 index.

    Articles use negative rowids to avoid collision with fact rowids.
    """
    await execute(
        "INSERT OR REPLACE INTO kb_search (rowid, fact_id, article_id, title, content) "
        "VALUES (?, ?, ?, ?, ?)",
        (-article_id, "", str(article_id), title, content),
    )


async def search_kb(
    query: str,
    topic: str | None = None,
    country_iso3: str | None = None,
    include_stale: bool = False,
    limit: int = 10,
) -> dict:
    """Search the knowledge base using FTS5.

    Returns {facts: [...], articles: [...]}.
    """
    if not query.strip():
        return {"facts": [], "articles": []}

    # Escape FTS5 special characters for safety
    safe_query = query.replace('"', '""')

    # Search FTS5 index
    fts_rows = await fetch_all(
        'SELECT rowid, fact_id, article_id, rank FROM kb_search WHERE kb_search MATCH ? '
        'ORDER BY rank LIMIT ?',
        (f'"{safe_query}"', limit * 2),
    )

    fact_ids = []
    article_ids = []
    for row in fts_rows:
        if row["fact_id"]:
            fact_ids.append(int(row["fact_id"]))
        if row["article_id"]:
            article_ids.append(int(row["article_id"]))

    # Fetch full fact records with filtering
    facts = []
    if fact_ids:
        placeholders = ",".join("?" for _ in fact_ids)
        conditions = [f"id IN ({placeholders})"]
        params: list = list(fact_ids)

        if topic:
            conditions.append("topic = ?")
            params.append(topic)
        if country_iso3:
            conditions.append("country_iso3 = ?")
            params.append(country_iso3.upper())
        if not include_stale:
            conditions.append("is_stale = 0")

        where = " AND ".join(conditions)
        rows = await fetch_all(
            f"SELECT id as fact_id, claim, topic, subtopic, country_iso3, confidence "
            f"FROM kb_facts WHERE {where} ORDER BY confidence DESC LIMIT ?",
            tuple(params + [limit]),
        )
        facts = rows

    # Fetch full article records with filtering
    articles = []
    if article_ids:
        placeholders = ",".join("?" for _ in article_ids)
        conditions = [f"id IN ({placeholders})"]
        params = list(article_ids)

        if topic:
            conditions.append("topic = ?")
            params.append(topic)
        if country_iso3:
            conditions.append("country_iso3 = ?")
            params.append(country_iso3.upper())

        where = " AND ".join(conditions)
        rows = await fetch_all(
            f"SELECT id as article_id, slug, title, summary, topic, country_iso3, fact_count "
            f"FROM kb_articles WHERE {where} LIMIT ?",
            tuple(params + [5]),
        )
        articles = rows

    return {"facts": facts, "articles": articles}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/mddeluairhossen/equilibria && uv run pytest tests/kb/test_search.py -v`
Expected: All 8 tests PASS

- [ ] **Step 5: Commit**

```bash
git add app/kb/search.py tests/kb/test_search.py
git commit -m "add FTS5 search helpers for knowledge base"
```

---

### Task 3: Fact Extractor (Stage 1)

**Files:**
- Create: `app/kb/extractor.py`
- Create: `tests/kb/test_extractor.py`

- [ ] **Step 1: Write failing tests**

Create `tests/kb/test_extractor.py`:

```python
"""Tests for Stage 1: fact extraction."""

import json

import pytest

from app.db import execute, fetch_all, fetch_one
from app.kb.extractor import extract_facts_from_result, store_fact


@pytest.mark.asyncio
async def test_store_fact(tmp_db):
    """store_fact inserts into kb_facts and kb_sources."""
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
    """store_fact also populates the FTS5 index."""
    fact_id = await store_fact(
        claim="Textile exports grew 12%",
        topic="trade",
        confidence=0.8,
        evidence=[],
        source_type="briefing",
        source_id=1,
    )
    fts = await fetch_all(
        "SELECT * FROM kb_search WHERE kb_search MATCH 'textile'",
    )
    assert len(fts) >= 1


@pytest.mark.asyncio
async def test_extract_facts_from_result_dict(tmp_db):
    """extract_facts_from_result processes a result dict."""
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
    # May be empty if no API key, but should not error
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/mddeluairhossen/equilibria && uv run pytest tests/kb/test_extractor.py -v`
Expected: FAIL with "cannot import name 'extract_facts_from_result'"

- [ ] **Step 3: Implement extractor.py**

Create `app/kb/extractor.py`:

```python
"""Stage 1: Extract facts from analysis results, briefings, and conversations."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import anthropic

from app.config import settings
from app.db import execute, fetch_all, fetch_one
from app.kb.search import index_fact

logger = logging.getLogger(__name__)

EXTRACTION_PROMPT = """Extract factual, verifiable economic claims from the following analysis result.
Return a JSON array of objects: [{claim, topic, subtopic, country_iso3, confidence}]
- claim: a specific factual statement (e.g. "Bangladesh RCA in textiles is 12.3")
- topic: one of trade, macro, labor, development, agricultural, financial, health, environmental, public, spatial, political, behavioral, industrial, monetary, energy, demographic, methods
- subtopic: specific analysis type (e.g. rca, gravity, phillips_curve)
- country_iso3: ISO3 code or null for global
- confidence: 0.0-1.0 based on data quality

Only extract verifiable facts, not opinions or hedged language. Return [] if no clear facts.

Analysis result:
{data}"""

LAYER_TO_TOPIC = {
    "l1": "trade", "l2": "macro", "l3": "labor", "l4": "development",
    "l5": "agricultural", "l6": "integration", "l7": "financial", "l8": "health",
    "l9": "environmental", "l10": "public", "l11": "spatial", "l12": "political",
    "l13": "behavioral", "l14": "industrial", "l15": "monetary", "l16": "energy",
    "l17": "demographic", "l18": "methods",
}


async def store_fact(
    claim: str,
    topic: str,
    confidence: float,
    evidence: list[dict],
    source_type: str,
    source_id: int,
    subtopic: str | None = None,
    country_iso3: str | None = None,
) -> int:
    """Insert a fact into kb_facts, link source, and index in FTS5. Returns fact_id."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    await execute(
        "INSERT INTO kb_facts (claim, topic, subtopic, country_iso3, confidence, evidence, "
        "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (claim, topic, subtopic, country_iso3, confidence, json.dumps(evidence), now, now),
    )
    row = await fetch_one("SELECT last_insert_rowid() as id")
    fact_id = row["id"]

    await execute(
        "INSERT INTO kb_sources (fact_id, source_type, source_id, source_date) VALUES (?, ?, ?, ?)",
        (fact_id, source_type, source_id, now),
    )

    await index_fact(fact_id, claim)
    return fact_id


async def extract_facts_from_result(result: dict) -> list[int]:
    """Use Claude to extract facts from an analysis result. Returns list of fact_ids."""
    if not settings.anthropic_api_key:
        logger.debug("No API key, skipping fact extraction")
        return []

    data_str = json.dumps(result, default=str)[:3000]
    try:
        client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
        response = await client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            messages=[{"role": "user", "content": EXTRACTION_PROMPT.format(data=data_str)}],
        )
        text = response.content[0].text
        # Parse JSON from response
        start = text.find("[")
        end = text.rfind("]") + 1
        if start == -1 or end == 0:
            return []
        claims = json.loads(text[start:end])
    except Exception:
        logger.exception("Fact extraction failed")
        return []

    layer = result.get("layer", "")
    default_topic = LAYER_TO_TOPIC.get(layer, result.get("analysis_type", "methods"))

    fact_ids = []
    for claim_data in claims:
        claim_text = claim_data.get("claim", "")
        if not claim_text:
            continue
        fact_id = await store_fact(
            claim=claim_text,
            topic=claim_data.get("topic", default_topic),
            subtopic=claim_data.get("subtopic", result.get("analysis_type")),
            country_iso3=claim_data.get("country_iso3", result.get("country_iso3")),
            confidence=claim_data.get("confidence", 0.5),
            evidence=[{
                "type": "analysis_result",
                "id": result.get("id", 0),
                "summary": f"{result.get('analysis_type', 'unknown')} analysis",
            }],
            source_type="analysis_result",
            source_id=result.get("id", 0),
        )
        fact_ids.append(fact_id)

    logger.info("Extracted %d facts from result id=%s", len(fact_ids), result.get("id"))
    return fact_ids


async def extract_facts_from_briefing(briefing: dict) -> list[int]:
    """Extract facts from a briefing record. Returns list of fact_ids."""
    if not settings.anthropic_api_key:
        return []

    content = briefing.get("content", "")[:4000]
    try:
        client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
        response = await client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            messages=[{"role": "user", "content": EXTRACTION_PROMPT.format(data=content)}],
        )
        text = response.content[0].text
        start = text.find("[")
        end = text.rfind("]") + 1
        if start == -1 or end == 0:
            return []
        claims = json.loads(text[start:end])
    except Exception:
        logger.exception("Briefing fact extraction failed")
        return []

    fact_ids = []
    for claim_data in claims:
        claim_text = claim_data.get("claim", "")
        if not claim_text:
            continue
        fact_id = await store_fact(
            claim=claim_text,
            topic=claim_data.get("topic", "integration"),
            subtopic=claim_data.get("subtopic"),
            country_iso3=claim_data.get("country_iso3", briefing.get("country_iso3")),
            confidence=claim_data.get("confidence", 0.6),
            evidence=[{
                "type": "briefing",
                "id": briefing.get("id", 0),
                "summary": briefing.get("title", "briefing"),
            }],
            source_type="briefing",
            source_id=briefing.get("id", 0),
        )
        fact_ids.append(fact_id)

    return fact_ids


async def extract_new_facts(full: bool = False) -> int:
    """Scan analysis_results and briefings for new data, extract facts.

    If full=True, reprocess all sources. Otherwise only new since last compile.
    Returns total facts extracted.
    """
    last_compile = None
    if not full:
        row = await fetch_one(
            "SELECT finished_at FROM collection_log WHERE source = 'kb_compiler' "
            "ORDER BY finished_at DESC LIMIT 1"
        )
        last_compile = row["finished_at"] if row else None

    # Process analysis results
    if last_compile:
        results = await fetch_all(
            "SELECT id, analysis_type, country_iso3, layer, parameters, result, score, "
            "signal, created_at FROM analysis_results WHERE created_at > ? ORDER BY created_at",
            (last_compile,),
        )
    else:
        results = await fetch_all(
            "SELECT id, analysis_type, country_iso3, layer, parameters, result, score, "
            "signal, created_at FROM analysis_results ORDER BY created_at",
        )

    total = 0
    for result in results:
        fact_ids = await extract_facts_from_result(result)
        total += len(fact_ids)

    # Process briefings
    if last_compile:
        briefings = await fetch_all(
            "SELECT id, country_iso3, title, content, created_at FROM briefings "
            "WHERE created_at > ? ORDER BY created_at",
            (last_compile,),
        )
    else:
        briefings = await fetch_all(
            "SELECT id, country_iso3, title, content, created_at FROM briefings "
            "ORDER BY created_at",
        )

    for briefing in briefings:
        fact_ids = await extract_facts_from_briefing(briefing)
        total += len(fact_ids)

    logger.info("Total facts extracted: %d (from %d results, %d briefings)",
                total, len(results), len(briefings))
    return total
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/mddeluairhossen/equilibria && uv run pytest tests/kb/test_extractor.py -v`
Expected: All 3 tests PASS

- [ ] **Step 5: Commit**

```bash
git add app/kb/extractor.py tests/kb/test_extractor.py
git commit -m "add fact extractor (stage 1 of KB compiler)"
```

---

### Task 4: Article Compiler (Stage 2)

**Files:**
- Create: `app/kb/articler.py`
- Create: `tests/kb/test_articler.py`

- [ ] **Step 1: Write failing tests**

Create `tests/kb/test_articler.py`:

```python
"""Tests for Stage 2: article compilation."""

import pytest

from app.db import execute, fetch_all, fetch_one
from app.kb.articler import compile_article_for_group, get_compilable_groups


@pytest.mark.asyncio
async def test_get_compilable_groups_empty(tmp_db):
    """No facts means no compilable groups."""
    groups = await get_compilable_groups()
    assert groups == []


@pytest.mark.asyncio
async def test_get_compilable_groups_needs_three(tmp_db):
    """Groups need at least 3 facts to be compilable."""
    for i in range(2):
        await execute(
            "INSERT INTO kb_facts (claim, topic, country_iso3, confidence, evidence) "
            "VALUES (?, ?, ?, ?, ?)",
            (f"Fact {i}", "trade", "BGD", 0.8, "[]"),
        )
    groups = await get_compilable_groups()
    assert groups == []


@pytest.mark.asyncio
async def test_get_compilable_groups_with_three(tmp_db):
    """Groups with 3+ facts are returned."""
    for i in range(3):
        await execute(
            "INSERT INTO kb_facts (claim, topic, country_iso3, confidence, evidence) "
            "VALUES (?, ?, ?, ?, ?)",
            (f"Trade fact {i}", "trade", "BGD", 0.8, "[]"),
        )
    groups = await get_compilable_groups()
    assert len(groups) == 1
    assert groups[0]["topic"] == "trade"
    assert groups[0]["country_iso3"] == "BGD"
    assert groups[0]["count"] >= 3
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/mddeluairhossen/equilibria && uv run pytest tests/kb/test_articler.py -v`
Expected: FAIL with "cannot import name 'get_compilable_groups'"

- [ ] **Step 3: Implement articler.py**

Create `app/kb/articler.py`:

```python
"""Stage 2: Compile facts into markdown articles."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone

import anthropic

from app.config import settings
from app.db import execute, fetch_all, fetch_one
from app.kb.search import index_article

logger = logging.getLogger(__name__)

COMPILE_PROMPT = """You are compiling an economics knowledge base article from verified facts.

Topic: {topic}
Country: {country}

Facts:
{facts_text}

Write a concise markdown article with:
1. A clear title
2. A 2-3 sentence summary
3. A body that synthesizes these facts into a coherent narrative
4. Cite facts inline as [Fact #N]

Return JSON: {{"title": "...", "slug": "...", "summary": "...", "content": "..."}}
The slug should be URL-friendly lowercase with hyphens (e.g. "bangladesh-trade-competitiveness")."""


async def get_compilable_groups() -> list[dict]:
    """Find (topic, country_iso3) groups with 3+ non-stale facts."""
    return await fetch_all(
        "SELECT topic, country_iso3, COUNT(*) as count "
        "FROM kb_facts WHERE is_stale = 0 "
        "GROUP BY topic, country_iso3 "
        "HAVING COUNT(*) >= 3 "
        "ORDER BY count DESC"
    )


async def compile_article_for_group(topic: str, country_iso3: str | None) -> int | None:
    """Compile or update an article for a (topic, country) group. Returns article_id or None."""
    if not settings.anthropic_api_key:
        logger.debug("No API key, skipping article compilation")
        return None

    # Get facts for this group
    if country_iso3:
        facts = await fetch_all(
            "SELECT id, claim, subtopic, confidence FROM kb_facts "
            "WHERE topic = ? AND country_iso3 = ? AND is_stale = 0 ORDER BY confidence DESC",
            (topic, country_iso3),
        )
    else:
        facts = await fetch_all(
            "SELECT id, claim, subtopic, confidence FROM kb_facts "
            "WHERE topic = ? AND country_iso3 IS NULL AND is_stale = 0 ORDER BY confidence DESC",
            (topic,),
        )

    if len(facts) < 3:
        return None

    # Check if article already exists
    if country_iso3:
        existing = await fetch_one(
            "SELECT id, updated_at FROM kb_articles WHERE topic = ? AND country_iso3 = ?",
            (topic, country_iso3),
        )
    else:
        existing = await fetch_one(
            "SELECT id, updated_at FROM kb_articles WHERE topic = ? AND country_iso3 IS NULL",
            (topic,),
        )

    # Build facts text for prompt
    facts_text = "\n".join(
        f"[Fact #{f['id']}] (confidence: {f['confidence']:.1f}) {f['claim']}"
        for f in facts[:20]  # Cap at 20 facts per article
    )

    country_label = country_iso3 or "Global"

    try:
        client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
        response = await client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            messages=[{
                "role": "user",
                "content": COMPILE_PROMPT.format(
                    topic=topic, country=country_label, facts_text=facts_text
                ),
            }],
        )
        text = response.content[0].text
        start = text.find("{")
        end = text.rfind("}") + 1
        if start == -1 or end == 0:
            return None
        article_data = json.loads(text[start:end])
    except Exception:
        logger.exception("Article compilation failed for %s/%s", topic, country_iso3)
        return None

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    title = article_data.get("title", f"{topic.title()} - {country_label}")
    slug = article_data.get("slug", f"{topic}-{country_label}".lower().replace(" ", "-"))
    # Sanitize slug
    slug = re.sub(r"[^a-z0-9-]", "", slug)
    summary = article_data.get("summary", "")
    content = article_data.get("content", "")

    if existing:
        article_id = existing["id"]
        await execute(
            "UPDATE kb_articles SET title = ?, content = ?, summary = ?, "
            "fact_count = ?, updated_at = ? WHERE id = ?",
            (title, content, summary, len(facts), now, article_id),
        )
        # Clear old fact links
        await execute("DELETE FROM kb_article_facts WHERE article_id = ?", (article_id,))
    else:
        await execute(
            "INSERT INTO kb_articles (slug, title, topic, country_iso3, content, summary, "
            "fact_count, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (slug, title, topic, country_iso3, content, summary, len(facts), now, now),
        )
        row = await fetch_one("SELECT last_insert_rowid() as id")
        article_id = row["id"]

    # Link facts to article
    for fact in facts:
        await execute(
            "INSERT OR IGNORE INTO kb_article_facts (article_id, fact_id) VALUES (?, ?)",
            (article_id, fact["id"]),
        )

    await index_article(article_id, title, content)
    logger.info("Compiled article id=%d slug=%s with %d facts", article_id, slug, len(facts))
    return article_id


async def compile_articles() -> int:
    """Compile articles for all eligible groups. Returns number of articles compiled."""
    groups = await get_compilable_groups()
    compiled = 0
    for group in groups:
        article_id = await compile_article_for_group(group["topic"], group["country_iso3"])
        if article_id:
            compiled += 1
    logger.info("Compiled %d articles from %d groups", compiled, len(groups))
    return compiled
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/mddeluairhossen/equilibria && uv run pytest tests/kb/test_articler.py -v`
Expected: All 3 tests PASS

- [ ] **Step 5: Commit**

```bash
git add app/kb/articler.py tests/kb/test_articler.py
git commit -m "add article compiler (stage 2 of KB compiler)"
```

---

### Task 5: Staleness Sweep (Stage 3) + Compiler Orchestrator

**Files:**
- Create: `app/kb/staleness.py`
- Create: `app/kb/compiler.py`
- Create: `tests/kb/test_staleness.py`

- [ ] **Step 1: Write failing tests**

Create `tests/kb/test_staleness.py`:

```python
"""Tests for Stage 3: staleness sweep and compiler orchestrator."""

import pytest

from app.db import execute, fetch_one
from app.kb.staleness import sweep_staleness


@pytest.mark.asyncio
async def test_sweep_marks_stale_facts(tmp_db):
    """Facts past stale_at get marked is_stale=1."""
    await execute(
        "INSERT INTO kb_facts (id, claim, topic, confidence, evidence, stale_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (1, "Old fact", "trade", 0.8, "[]", "2020-01-01 00:00:00"),
    )
    swept = await sweep_staleness()
    assert swept > 0
    fact = await fetch_one("SELECT is_stale, confidence FROM kb_facts WHERE id = 1")
    assert fact["is_stale"] == 1
    assert fact["confidence"] < 0.8


@pytest.mark.asyncio
async def test_sweep_ignores_fresh_facts(tmp_db):
    """Facts with future stale_at are not marked stale."""
    await execute(
        "INSERT INTO kb_facts (id, claim, topic, confidence, evidence, stale_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (1, "Fresh fact", "trade", 0.8, "[]", "2099-01-01 00:00:00"),
    )
    swept = await sweep_staleness()
    assert swept == 0
    fact = await fetch_one("SELECT is_stale FROM kb_facts WHERE id = 1")
    assert fact["is_stale"] == 0


@pytest.mark.asyncio
async def test_sweep_confidence_floor(tmp_db):
    """Confidence cannot go below 0.1."""
    await execute(
        "INSERT INTO kb_facts (id, claim, topic, confidence, evidence, stale_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (1, "Low confidence fact", "trade", 0.1, "[]", "2020-01-01 00:00:00"),
    )
    await sweep_staleness()
    fact = await fetch_one("SELECT confidence FROM kb_facts WHERE id = 1")
    assert fact["confidence"] == 0.1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/mddeluairhossen/equilibria && uv run pytest tests/kb/test_staleness.py -v`
Expected: FAIL with "cannot import name 'sweep_staleness'"

- [ ] **Step 3: Implement staleness.py**

Create `app/kb/staleness.py`:

```python
"""Stage 3: Staleness sweep for knowledge base facts."""

from __future__ import annotations

import logging

from app.db import execute, fetch_all

logger = logging.getLogger(__name__)


async def sweep_staleness() -> int:
    """Mark expired facts as stale and decay confidence. Returns count of facts marked stale."""
    # Find facts that have passed their stale_at and aren't already stale
    expired = await fetch_all(
        "SELECT id, confidence FROM kb_facts "
        "WHERE is_stale = 0 AND stale_at < datetime('now')"
    )

    for fact in expired:
        new_confidence = max(fact["confidence"] - 0.1, 0.1)
        await execute(
            "UPDATE kb_facts SET is_stale = 1, confidence = ?, updated_at = datetime('now') "
            "WHERE id = ?",
            (round(new_confidence, 2), fact["id"]),
        )

    if expired:
        logger.info("Marked %d facts as stale", len(expired))
    return len(expired)


async def refresh_from_sources() -> int:
    """Check if source data has been updated; refresh linked facts. Returns refreshed count."""
    # For each non-stale source link, check if the underlying data is newer
    sources = await fetch_all(
        "SELECT ks.fact_id, ks.source_type, ks.source_id, ks.source_date, kf.confidence "
        "FROM kb_sources ks "
        "JOIN kb_facts kf ON ks.fact_id = kf.id "
        "WHERE kf.is_stale = 0"
    )

    refreshed = 0
    for src in sources:
        # Check if source has newer data
        if src["source_type"] == "analysis_result":
            row = await fetch_all(
                "SELECT created_at FROM analysis_results WHERE id = ? AND created_at > ?",
                (src["source_id"], src["source_date"]),
            )
        elif src["source_type"] == "briefing":
            row = await fetch_all(
                "SELECT created_at FROM briefings WHERE id = ? AND created_at > ?",
                (src["source_id"], src["source_date"]),
            )
        elif src["source_type"] == "data_point":
            row = await fetch_all(
                "SELECT created_at FROM data_points WHERE id = ? AND created_at > ?",
                (src["source_id"], src["source_date"]),
            )
        else:
            continue

        if row:
            new_confidence = min(src["confidence"] + 0.05, 1.0)
            await execute(
                "UPDATE kb_facts SET stale_at = datetime('now', '+30 days'), "
                "confidence = ?, updated_at = datetime('now') WHERE id = ?",
                (round(new_confidence, 2), src["fact_id"]),
            )
            await execute(
                "UPDATE kb_sources SET source_date = datetime('now') WHERE fact_id = ? "
                "AND source_type = ? AND source_id = ?",
                (src["fact_id"], src["source_type"], src["source_id"]),
            )
            refreshed += 1

    if refreshed:
        logger.info("Refreshed %d facts from updated sources", refreshed)
    return refreshed
```

- [ ] **Step 4: Implement compiler.py orchestrator**

Create `app/kb/compiler.py`:

```python
"""Knowledge base compiler orchestrator."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from app.db import execute
from app.kb.articler import compile_articles
from app.kb.extractor import extract_new_facts
from app.kb.staleness import refresh_from_sources, sweep_staleness

logger = logging.getLogger(__name__)


async def compile_kb(full: bool = False) -> dict:
    """Run all three compiler stages.

    Stage 1: Extract facts from new analysis_results and briefings
    Stage 2: Compile/update articles from fact groups
    Stage 3: Sweep staleness and refresh from sources

    Args:
        full: If True, reprocess all sources, not just new ones.

    Returns dict with counts for each stage.
    """
    started_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Stage 1: Extract facts
    facts_extracted = await extract_new_facts(full=full)

    # Stage 2: Compile articles
    articles_compiled = await compile_articles()

    # Stage 3: Staleness
    refreshed = await refresh_from_sources()
    swept = await sweep_staleness()

    finished_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Log the compile run
    await execute(
        "INSERT INTO collection_log (source, series_count, point_count, status, started_at, finished_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        ("kb_compiler", articles_compiled, facts_extracted, "success", started_at, finished_at),
    )

    result = {
        "facts_extracted": facts_extracted,
        "articles_compiled": articles_compiled,
        "facts_refreshed": refreshed,
        "facts_swept": swept,
        "started_at": started_at,
        "finished_at": finished_at,
    }
    logger.info("KB compile complete: %s", result)
    return result
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd /Users/mddeluairhossen/equilibria && uv run pytest tests/kb/ -v`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add app/kb/staleness.py app/kb/compiler.py tests/kb/test_staleness.py
git commit -m "add staleness sweep (stage 3) and compiler orchestrator"
```

---

### Task 6: AI Brain Tools — search_knowledge and file_insight

**Files:**
- Modify: `app/ai/tools.py`
- Modify: `app/ai/brain.py`

- [ ] **Step 1: Write failing tests**

Create `tests/kb/test_tools.py`:

```python
"""Tests for KB brain tools."""

import pytest

from app.ai.tools import execute_tool, TOOL_REGISTRY
from app.db import execute


@pytest.mark.asyncio
async def test_search_knowledge_tool_exists():
    """search_knowledge is registered in TOOL_REGISTRY."""
    assert "search_knowledge" in TOOL_REGISTRY


@pytest.mark.asyncio
async def test_file_insight_tool_exists():
    """file_insight is registered in TOOL_REGISTRY."""
    assert "file_insight" in TOOL_REGISTRY


@pytest.mark.asyncio
async def test_search_knowledge_empty(tmp_db):
    """search_knowledge returns empty on empty KB."""
    result = await execute_tool("search_knowledge", {"query": "trade"})
    assert result["facts"] == []
    assert result["articles"] == []


@pytest.mark.asyncio
async def test_file_insight_creates_fact(tmp_db):
    """file_insight inserts a new fact."""
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
    """After filing an insight, search_knowledge finds it."""
    await execute_tool("file_insight", {
        "claim": "Bangladesh has strong textile comparative advantage",
        "topic": "trade",
        "evidence": [{"type": "analysis_result", "id": 1, "summary": "RCA"}],
    })
    result = await execute_tool("search_knowledge", {"query": "textile comparative advantage"})
    assert len(result["facts"]) >= 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/mddeluairhossen/equilibria && uv run pytest tests/kb/test_tools.py -v`
Expected: FAIL with "KeyError: 'search_knowledge'"

- [ ] **Step 3: Add tools to TOOL_REGISTRY**

In `app/ai/tools.py`, add the following two tool implementations before the `TOOL_REGISTRY` dict:

```python
async def search_knowledge(
    query: str,
    topic: str | None = None,
    country_iso3: str | None = None,
    include_stale: bool = False,
) -> dict:
    """Search the accumulated knowledge base for economic insights."""
    from app.kb.search import search_kb

    results = await search_kb(
        query=query,
        topic=topic,
        country_iso3=country_iso3,
        include_stale=include_stale,
    )
    return {
        **results,
        "_citation": "Equilibria Knowledge Base",
    }


async def file_insight(
    claim: str,
    topic: str,
    evidence: list[dict],
    subtopic: str | None = None,
    country_iso3: str | None = None,
) -> dict:
    """File a new insight into the knowledge base."""
    from app.kb.extractor import store_fact
    from app.kb.search import search_kb

    # Dedup check
    existing = await search_kb(claim, topic=topic, country_iso3=country_iso3)
    for fact in existing.get("facts", []):
        if fact.get("claim", "").lower() == claim.lower():
            return {"fact_id": fact["fact_id"], "status": "duplicate"}

    fact_id = await store_fact(
        claim=claim,
        topic=topic,
        subtopic=subtopic,
        country_iso3=country_iso3,
        confidence=0.7,
        evidence=evidence,
        source_type="conversation",
        source_id=0,
    )
    return {"fact_id": fact_id, "status": "created"}
```

Then add entries to the `TOOL_REGISTRY` dict, before the closing brace:

```python
    "search_knowledge": {
        "fn": search_knowledge,
        "description": "Search the accumulated knowledge base for economic insights and articles. Use this BEFORE running analysis tools to check if the answer already exists.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query"},
                "topic": {"type": "string", "description": "Filter by topic (trade, macro, labor, development, agricultural, financial, health, environmental, public, spatial, political, behavioral, industrial, monetary, energy, demographic, methods)"},
                "country_iso3": {"type": "string", "description": "Filter by country ISO3 code"},
                "include_stale": {"type": "boolean", "description": "Include stale facts (default: false)"},
            },
            "required": ["query"],
        },
    },
    "file_insight": {
        "fn": file_insight,
        "description": "File a new economic insight discovered during analysis. Use when your analysis produces a novel finding worth preserving in the knowledge base.",
        "input_schema": {
            "type": "object",
            "properties": {
                "claim": {"type": "string", "description": "The factual claim to store"},
                "topic": {"type": "string", "description": "Topic category (trade, macro, labor, etc.)"},
                "subtopic": {"type": "string", "description": "Specific subtopic (rca, gravity, phillips_curve, etc.)"},
                "country_iso3": {"type": "string", "description": "Country ISO3 code, or omit for global insights"},
                "evidence": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "type": {"type": "string", "description": "Source type: analysis_result, data_point, briefing, conversation"},
                            "id": {"type": "integer", "description": "Source row ID"},
                            "summary": {"type": "string", "description": "Brief description of the source"},
                        },
                    },
                    "description": "Evidence supporting this insight",
                },
            },
            "required": ["claim", "topic", "evidence"],
        },
    },
```

- [ ] **Step 4: Update brain.py system prompt**

In `app/ai/brain.py`, change the `SYSTEM_PROMPT` constant:

```python
SYSTEM_PROMPT = (
    "You are Equilibria, an AI applied economics analyst with access to tools "
    "covering trade, macro, labor, development, and agricultural economics. "
    "Always use tools to get data before making claims. Cite sources. "
    "Be direct and analytical.\n\n"
    "You have access to a knowledge base of accumulated economic insights.\n"
    "- Before running analysis tools, search the knowledge base with search_knowledge.\n"
    "- If you find a recent, high-confidence match (confidence > 0.7), use it and cite 'KB fact #N'.\n"
    "- When your analysis produces a novel finding, file it with file_insight.\n"
    "- Prefer fresh facts over stale ones. Note staleness if citing older facts."
)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd /Users/mddeluairhossen/equilibria && uv run pytest tests/kb/test_tools.py -v`
Expected: All 5 tests PASS

- [ ] **Step 6: Commit**

```bash
git add app/ai/tools.py app/ai/brain.py tests/kb/test_tools.py
git commit -m "add search_knowledge and file_insight brain tools"
```

---

### Task 7: REST API Endpoints

**Files:**
- Create: `app/api/kb.py`
- Modify: `app/main.py`
- Create: `tests/api/test_kb.py`

- [ ] **Step 1: Write failing tests**

Create `tests/api/test_kb.py`:

```python
"""Tests for /api/kb endpoints."""

import pytest

from app.db import execute


@pytest.mark.asyncio
async def test_kb_stats_empty(async_client):
    """GET /api/kb/stats returns zero counts on empty KB."""
    resp = await async_client.get("/api/kb/stats")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_facts"] == 0
    assert data["total_articles"] == 0


@pytest.mark.asyncio
async def test_kb_articles_empty(async_client):
    """GET /api/kb/articles returns empty list."""
    resp = await async_client.get("/api/kb/articles")
    assert resp.status_code == 200
    assert resp.json()["articles"] == []


@pytest.mark.asyncio
async def test_kb_facts_empty(async_client):
    """GET /api/kb/facts returns empty list."""
    resp = await async_client.get("/api/kb/facts")
    assert resp.status_code == 200
    assert resp.json()["facts"] == []


@pytest.mark.asyncio
async def test_kb_search_empty(async_client):
    """GET /api/kb/search returns empty on empty KB."""
    resp = await async_client.get("/api/kb/search", params={"q": "trade"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["facts"] == []
    assert data["articles"] == []


@pytest.mark.asyncio
async def test_kb_article_not_found(async_client):
    """GET /api/kb/articles/nonexistent returns 404."""
    resp = await async_client.get("/api/kb/articles/nonexistent-slug")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_kb_compile_returns_200_or_503(async_client):
    """POST /api/kb/compile returns 200 (may be empty if no API key)."""
    resp = await async_client.post("/api/kb/compile")
    assert resp.status_code in (200, 503)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/mddeluairhossen/equilibria && uv run pytest tests/api/test_kb.py -v`
Expected: FAIL with 404 (routes not registered)

- [ ] **Step 3: Implement kb.py API**

Create `app/api/kb.py`:

```python
"""Knowledge Base API routes."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.db import fetch_all, fetch_one
from app.kb.compiler import compile_kb
from app.kb.search import search_kb

router = APIRouter(prefix="/kb", tags=["knowledge-base"])


@router.get("/stats")
async def kb_stats():
    """Knowledge base statistics."""
    facts_row = await fetch_one("SELECT COUNT(*) as total FROM kb_facts")
    articles_row = await fetch_one("SELECT COUNT(*) as total FROM kb_articles")
    stale_row = await fetch_one("SELECT COUNT(*) as total FROM kb_facts WHERE is_stale = 1")
    fresh_row = await fetch_one("SELECT COUNT(*) as total FROM kb_facts WHERE is_stale = 0")
    last_compile = await fetch_one(
        "SELECT finished_at FROM collection_log WHERE source = 'kb_compiler' "
        "ORDER BY finished_at DESC LIMIT 1"
    )
    topics = await fetch_all(
        "SELECT topic, COUNT(*) as count FROM kb_facts GROUP BY topic ORDER BY count DESC"
    )
    return {
        "total_facts": facts_row["total"] if facts_row else 0,
        "total_articles": articles_row["total"] if articles_row else 0,
        "stale_facts": stale_row["total"] if stale_row else 0,
        "fresh_facts": fresh_row["total"] if fresh_row else 0,
        "last_compile": last_compile["finished_at"] if last_compile else None,
        "facts_by_topic": topics,
    }


@router.get("/articles")
async def kb_articles(
    topic: str | None = None,
    country_iso3: str | None = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
):
    """List KB articles with optional filters."""
    conditions = []
    params: list = []
    if topic:
        conditions.append("topic = ?")
        params.append(topic)
    if country_iso3:
        conditions.append("country_iso3 = ?")
        params.append(country_iso3.upper())
    where = " AND ".join(conditions) if conditions else "1=1"
    offset = (page - 1) * per_page

    articles = await fetch_all(
        f"SELECT id, slug, title, topic, country_iso3, summary, fact_count, "
        f"created_at, updated_at FROM kb_articles WHERE {where} "
        f"ORDER BY updated_at DESC LIMIT ? OFFSET ?",
        tuple(params + [per_page, offset]),
    )
    total_row = await fetch_one(
        f"SELECT COUNT(*) as total FROM kb_articles WHERE {where}",
        tuple(params),
    )
    return {
        "articles": articles,
        "total": total_row["total"] if total_row else 0,
        "page": page,
        "per_page": per_page,
    }


@router.get("/articles/{slug}")
async def kb_article_detail(slug: str):
    """Get a single KB article by slug."""
    article = await fetch_one(
        "SELECT * FROM kb_articles WHERE slug = ?", (slug,)
    )
    if not article:
        raise HTTPException(status_code=404, detail="Article not found")

    # Get linked facts
    facts = await fetch_all(
        "SELECT f.id, f.claim, f.topic, f.subtopic, f.confidence, f.is_stale "
        "FROM kb_facts f "
        "JOIN kb_article_facts af ON f.id = af.fact_id "
        "WHERE af.article_id = ? ORDER BY f.confidence DESC",
        (article["id"],),
    )
    return {**article, "facts": facts}


@router.get("/facts")
async def kb_facts(
    topic: str | None = None,
    country_iso3: str | None = None,
    is_stale: bool | None = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
):
    """List KB facts with optional filters."""
    conditions = []
    params: list = []
    if topic:
        conditions.append("topic = ?")
        params.append(topic)
    if country_iso3:
        conditions.append("country_iso3 = ?")
        params.append(country_iso3.upper())
    if is_stale is not None:
        conditions.append("is_stale = ?")
        params.append(1 if is_stale else 0)
    where = " AND ".join(conditions) if conditions else "1=1"
    offset = (page - 1) * per_page

    facts = await fetch_all(
        f"SELECT id, claim, topic, subtopic, country_iso3, confidence, is_stale, "
        f"created_at, updated_at FROM kb_facts WHERE {where} "
        f"ORDER BY created_at DESC LIMIT ? OFFSET ?",
        tuple(params + [per_page, offset]),
    )
    total_row = await fetch_one(
        f"SELECT COUNT(*) as total FROM kb_facts WHERE {where}",
        tuple(params),
    )
    return {
        "facts": facts,
        "total": total_row["total"] if total_row else 0,
        "page": page,
        "per_page": per_page,
    }


@router.get("/search")
async def kb_search(
    q: str = Query(..., min_length=1),
    topic: str | None = None,
    country_iso3: str | None = None,
):
    """Full-text search across facts and articles."""
    return await search_kb(query=q, topic=topic, country_iso3=country_iso3)


@router.post("/compile")
async def kb_compile(full: bool = False):
    """Trigger the knowledge base compiler."""
    result = await compile_kb(full=full)
    return result
```

- [ ] **Step 4: Register router in main.py**

In `app/main.py`, add to the `_router_modules` list (after the chat entry):

```python
    ("app.api.kb", "/api", "Knowledge Base"),
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd /Users/mddeluairhossen/equilibria && uv run pytest tests/api/test_kb.py -v`
Expected: All 6 tests PASS

- [ ] **Step 6: Commit**

```bash
git add app/api/kb.py app/main.py tests/api/test_kb.py
git commit -m "add knowledge base REST API endpoints"
```

---

### Task 8: Post-Collect and Post-Generate Hooks

**Files:**
- Modify: `app/collectors/base.py`
- Modify: `app/briefings/base.py`

- [ ] **Step 1: Add post-collect hook to BaseCollector.run()**

In `app/collectors/base.py`, modify the `run` method to trigger fact extraction after a successful collection. Add the import at the top:

```python
import logging
```

Change the `run` method's success path. After the line `self.logger.info(f"[{self.name}] collected={len(raw)} valid={len(valid)} stored={stored}")`, add:

```python
            # Trigger KB fact extraction for new data
            try:
                from app.kb.extractor import extract_new_facts
                await extract_new_facts()
            except Exception:
                self.logger.debug("KB fact extraction skipped")
```

The full modified `run` method becomes:

```python
    async def run(self) -> dict:
        """Execute collect -> validate -> store pipeline."""
        try:
            self.logger.info(f"[{self.name}] collecting...")
            result = await self.collect()
            if isinstance(result, dict):
                self.logger.info(f"[{self.name}] done: {result}")
                return {"status": "success", **result}
            raw = result
            valid = await self.validate(raw)
            stored = await self.store(valid)
            self.logger.info(
                f"[{self.name}] collected={len(raw)} valid={len(valid)} stored={stored}"
            )
            # Trigger KB fact extraction for new data
            try:
                from app.kb.extractor import extract_new_facts
                await extract_new_facts()
            except Exception:
                self.logger.debug("KB fact extraction skipped")
            return {
                "status": "success",
                "collected": len(raw),
                "valid": len(valid),
                "stored": stored,
            }
        except Exception as e:
            self.logger.error(f"[{self.name}] failed: {e}")
            return {"status": "failed", "error": str(e)}
```

- [ ] **Step 2: Add post-generate hook to BriefingBase.save()**

In `app/briefings/base.py`, after the `logger.info("Saved briefing id=%d type=%s", row_id, result["briefing_type"])` line in the `save` method, add:

```python
        # Trigger KB fact extraction for this briefing
        try:
            from app.kb.extractor import extract_facts_from_briefing
            briefing_data = {
                "id": row_id,
                "country_iso3": country_iso3,
                "title": result["title"],
                "content": result["body_html"],
            }
            await extract_facts_from_briefing(briefing_data)
        except Exception:
            logger.debug("KB fact extraction from briefing skipped")
```

- [ ] **Step 3: Run existing tests to confirm no regressions**

Run: `cd /Users/mddeluairhossen/equilibria && uv run pytest tests/collectors/ tests/briefings/ -v`
Expected: All existing tests PASS

- [ ] **Step 4: Commit**

```bash
git add app/collectors/base.py app/briefings/base.py
git commit -m "add post-collect and post-generate hooks for KB extraction"
```

---

### Task 9: Frontend — Knowledge Base Index Page

**Files:**
- Create: `web/app/knowledge/page.tsx`

- [ ] **Step 1: Create the KB index page**

Create `web/app/knowledge/page.tsx`:

```tsx
"use client";

import { useEffect, useState } from "react";
import Link from "next/link";

interface Article {
  id: number;
  slug: string;
  title: string;
  topic: string;
  country_iso3: string | null;
  summary: string;
  fact_count: number;
  updated_at: string;
}

interface KBStats {
  total_facts: number;
  total_articles: number;
  fresh_facts: number;
  stale_facts: number;
  last_compile: string | null;
  facts_by_topic: { topic: string; count: number }[];
}

const TOPICS = [
  "trade", "macro", "labor", "development", "agricultural", "financial",
  "health", "environmental", "public", "spatial", "political", "behavioral",
  "industrial", "monetary", "energy", "demographic", "methods",
];

export default function KnowledgePage() {
  const [articles, setArticles] = useState<Article[]>([]);
  const [stats, setStats] = useState<KBStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");
  const [topicFilter, setTopicFilter] = useState<string | null>(null);
  const [searchResults, setSearchResults] = useState<{ facts: any[]; articles: Article[] } | null>(null);

  useEffect(() => {
    Promise.allSettled([
      fetch("/api/kb/articles").then((r) => r.ok ? r.json() : null),
      fetch("/api/kb/stats").then((r) => r.ok ? r.json() : null),
    ]).then(([artRes, statRes]) => {
      if (artRes.status === "fulfilled" && artRes.value) setArticles(artRes.value.articles);
      if (statRes.status === "fulfilled" && statRes.value) setStats(statRes.value);
    }).finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    if (!search.trim()) { setSearchResults(null); return; }
    const t = setTimeout(() => {
      const params = new URLSearchParams({ q: search });
      if (topicFilter) params.set("topic", topicFilter);
      fetch(`/api/kb/search?${params}`)
        .then((r) => r.ok ? r.json() : null)
        .then((d) => { if (d) setSearchResults(d); })
        .catch(() => {});
    }, 300);
    return () => clearTimeout(t);
  }, [search, topicFilter]);

  const displayArticles = searchResults?.articles ?? articles;

  return (
    <div>
      <div className="mb-8">
        <span className="text-xs font-mono tracking-wider text-[var(--accent-primary)] uppercase">
          Knowledge Base
        </span>
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--text-primary)] mt-1">
          Economics Knowledge Base
        </h1>
        <p className="text-sm text-[var(--text-secondary)] mt-1">
          {stats ? (
            <>
              {stats.total_articles} articles, {stats.total_facts} facts
              {stats.last_compile && <> &middot; Last compiled {new Date(stats.last_compile).toLocaleDateString()}</>}
            </>
          ) : loading ? "Loading..." : "LLM-compiled insights from analysis runs, briefings, and conversations"}
        </p>
      </div>

      {/* Search */}
      <div className="mb-6">
        <input
          type="text"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Search facts and articles..."
          className="w-full px-4 py-2.5 rounded-lg bg-[var(--bg-card)] border border-[var(--border)] text-sm text-[var(--text-primary)] placeholder:text-[var(--text-muted)] focus:outline-none focus:border-[var(--accent-primary)]"
        />
      </div>

      {/* Topic Chips */}
      <div className="flex flex-wrap gap-2 mb-6">
        <button
          onClick={() => setTopicFilter(null)}
          className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
            !topicFilter
              ? "bg-[var(--accent-primary)] text-white"
              : "bg-[var(--bg-card)] text-[var(--text-secondary)] border border-[var(--border)] hover:border-[var(--accent-primary)]"
          }`}
        >
          All
        </button>
        {TOPICS.map((t) => (
          <button
            key={t}
            onClick={() => setTopicFilter(topicFilter === t ? null : t)}
            className={`px-3 py-1 rounded-full text-xs font-medium capitalize transition-colors ${
              topicFilter === t
                ? "bg-[var(--accent-primary)] text-white"
                : "bg-[var(--bg-card)] text-[var(--text-secondary)] border border-[var(--border)] hover:border-[var(--accent-primary)]"
            }`}
          >
            {t}
          </button>
        ))}
      </div>

      {/* Search Results: Facts */}
      {searchResults?.facts && searchResults.facts.length > 0 && (
        <div className="glass-card p-5 mb-6">
          <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-3">
            Matching Facts ({searchResults.facts.length})
          </h2>
          <div className="space-y-2">
            {searchResults.facts.map((f: any) => (
              <div key={f.fact_id} className="flex items-start justify-between py-2 border-b border-[var(--border)]/50 last:border-0">
                <div className="flex-1">
                  <p className="text-sm text-[var(--text-primary)]">{f.claim}</p>
                  <p className="text-xs text-[var(--text-muted)] mt-0.5">
                    {f.topic}{f.subtopic ? ` / ${f.subtopic}` : ""}{f.country_iso3 ? ` / ${f.country_iso3}` : ""}
                  </p>
                </div>
                <span className={`text-xs font-mono ml-3 ${f.confidence > 0.7 ? "text-emerald-600" : f.confidence > 0.4 ? "text-amber-600" : "text-red-500"}`}>
                  {(f.confidence * 100).toFixed(0)}%
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Article Grid */}
      {displayArticles.length > 0 ? (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {displayArticles.map((a) => (
            <Link key={a.slug} href={`/knowledge/${a.slug}`} className="no-underline">
              <div className="glass-card p-5 h-full hover:border-[var(--accent-primary)] transition-colors">
                <div className="flex items-center gap-2 mb-2">
                  <span className="text-[10px] font-mono tracking-wider uppercase px-2 py-0.5 rounded bg-[var(--accent-primary)]/10 text-[var(--accent-primary)]">
                    {a.topic}
                  </span>
                  {a.country_iso3 && (
                    <span className="text-[10px] font-mono text-[var(--text-muted)]">{a.country_iso3}</span>
                  )}
                </div>
                <h3 className="text-sm font-semibold text-[var(--text-primary)] mb-1">{a.title}</h3>
                <p className="text-xs text-[var(--text-secondary)] line-clamp-2">{a.summary}</p>
                <div className="flex items-center justify-between mt-3">
                  <span className="text-[10px] text-[var(--text-muted)]">{a.fact_count} facts</span>
                  <span className="text-[10px] text-[var(--text-muted)]">
                    {new Date(a.updated_at).toLocaleDateString()}
                  </span>
                </div>
              </div>
            </Link>
          ))}
        </div>
      ) : (
        <div className="glass-card p-8 text-center">
          <p className="text-sm text-[var(--text-muted)]">
            {loading ? "Loading..." : search ? "No results found" : "No articles yet. Run the compiler to generate articles from accumulated analysis data."}
          </p>
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 2: Build to verify no TypeScript errors**

Run: `cd /Users/mddeluairhossen/equilibria/web && npm run build`
Expected: Build succeeds

- [ ] **Step 3: Commit**

```bash
git add web/app/knowledge/page.tsx
git commit -m "add knowledge base index page with search and topic filters"
```

---

### Task 10: Frontend — Article Detail Page

**Files:**
- Create: `web/app/knowledge/[slug]/page.tsx`

- [ ] **Step 1: Create article detail page**

Create `web/app/knowledge/[slug]/page.tsx`:

```tsx
"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";

interface Fact {
  id: number;
  claim: string;
  topic: string;
  subtopic: string | null;
  confidence: number;
  is_stale: number;
}

interface ArticleDetail {
  id: number;
  slug: string;
  title: string;
  topic: string;
  country_iso3: string | null;
  content: string;
  summary: string;
  fact_count: number;
  created_at: string;
  updated_at: string;
  facts: Fact[];
}

export default function ArticleDetailPage() {
  const params = useParams();
  const slug = params.slug as string;
  const [article, setArticle] = useState<ArticleDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [notFound, setNotFound] = useState(false);

  useEffect(() => {
    fetch(`/api/kb/articles/${slug}`)
      .then((r) => {
        if (r.status === 404) { setNotFound(true); return null; }
        return r.ok ? r.json() : null;
      })
      .then((d) => { if (d) setArticle(d); })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [slug]);

  if (loading) {
    return <div className="text-sm text-[var(--text-muted)]">Loading...</div>;
  }

  if (notFound || !article) {
    return (
      <div className="glass-card p-8 text-center">
        <p className="text-sm text-[var(--text-muted)]">Article not found.</p>
        <Link href="/knowledge" className="text-sm text-[var(--accent-primary)] mt-2 inline-block">
          Back to Knowledge Base
        </Link>
      </div>
    );
  }

  const staleFacts = article.facts.filter((f) => f.is_stale).length;
  const staleRatio = article.facts.length > 0 ? staleFacts / article.facts.length : 0;

  return (
    <div className="flex gap-6">
      {/* Main Content */}
      <div className="flex-1 min-w-0">
        <div className="mb-4">
          <Link href="/knowledge" className="text-xs text-[var(--accent-primary)] hover:underline">
            &larr; Knowledge Base
          </Link>
        </div>

        <div className="mb-6">
          <div className="flex items-center gap-2 mb-2">
            <span className="text-[10px] font-mono tracking-wider uppercase px-2 py-0.5 rounded bg-[var(--accent-primary)]/10 text-[var(--accent-primary)]">
              {article.topic}
            </span>
            {article.country_iso3 && (
              <span className="text-xs font-mono text-[var(--text-muted)]">{article.country_iso3}</span>
            )}
          </div>
          <h1 className="text-2xl font-semibold tracking-tight text-[var(--text-primary)]">
            {article.title}
          </h1>
          <p className="text-sm text-[var(--text-secondary)] mt-1">{article.summary}</p>
          <p className="text-xs text-[var(--text-muted)] mt-2">
            Last compiled {new Date(article.updated_at).toLocaleDateString()}
            &middot; {article.fact_count} facts
          </p>
        </div>

        {staleRatio > 0.5 && (
          <div className="glass-card p-3 mb-4 border-l-4 border-l-[var(--accent-secondary)]">
            <p className="text-xs text-[var(--text-secondary)]">
              {Math.round(staleRatio * 100)}% of facts in this article are stale. Recompile to refresh.
            </p>
          </div>
        )}

        {/* Article Body */}
        <div className="glass-card p-6">
          <div className="prose prose-sm max-w-none text-[var(--text-primary)] [&_h1]:text-lg [&_h2]:text-base [&_h3]:text-sm [&_p]:text-sm [&_p]:leading-relaxed [&_ul]:text-sm [&_ol]:text-sm">
            {article.content.split("\n").map((line, i) => {
              if (line.startsWith("# ")) return <h1 key={i}>{line.slice(2)}</h1>;
              if (line.startsWith("## ")) return <h2 key={i}>{line.slice(3)}</h2>;
              if (line.startsWith("### ")) return <h3 key={i}>{line.slice(4)}</h3>;
              if (line.startsWith("- ")) return <li key={i}>{line.slice(2)}</li>;
              if (line.trim() === "") return <br key={i} />;
              return <p key={i}>{line}</p>;
            })}
          </div>
        </div>
      </div>

      {/* Sidebar: Facts */}
      <div className="w-72 shrink-0 hidden lg:block">
        <div className="glass-card p-4 sticky top-4">
          <h2 className="text-xs font-semibold text-[var(--text-primary)] uppercase tracking-wider mb-3">
            Source Facts ({article.facts.length})
          </h2>
          <div className="space-y-2 max-h-[70vh] overflow-y-auto">
            {article.facts.map((f) => (
              <div key={f.id} className="py-2 border-b border-[var(--border)]/50 last:border-0">
                <div className="flex items-center justify-between mb-1">
                  <span className="text-[10px] font-mono text-[var(--text-muted)]">#{f.id}</span>
                  <span className={`text-[10px] font-mono ${
                    f.is_stale ? "text-red-500" : f.confidence > 0.7 ? "text-emerald-600" : "text-amber-600"
                  }`}>
                    {f.is_stale ? "stale" : `${(f.confidence * 100).toFixed(0)}%`}
                  </span>
                </div>
                <p className="text-xs text-[var(--text-secondary)] leading-relaxed">{f.claim}</p>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Build to verify no TypeScript errors**

Run: `cd /Users/mddeluairhossen/equilibria/web && npm run build`
Expected: Build succeeds

- [ ] **Step 3: Commit**

```bash
git add web/app/knowledge/\\[slug\\]/page.tsx
git commit -m "add knowledge base article detail page"
```

---

### Task 11: Frontend — Fact Explorer Page

**Files:**
- Create: `web/app/knowledge/facts/page.tsx`

- [ ] **Step 1: Create fact explorer page**

Create `web/app/knowledge/facts/page.tsx`:

```tsx
"use client";

import { useEffect, useState } from "react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from "recharts";

interface Fact {
  id: number;
  claim: string;
  topic: string;
  subtopic: string | null;
  country_iso3: string | null;
  confidence: number;
  is_stale: number;
  created_at: string;
}

interface KBStats {
  total_facts: number;
  fresh_facts: number;
  stale_facts: number;
  facts_by_topic: { topic: string; count: number }[];
}

export default function FactExplorerPage() {
  const [facts, setFacts] = useState<Fact[]>([]);
  const [stats, setStats] = useState<KBStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [topicFilter, setTopicFilter] = useState<string | null>(null);
  const [staleFilter, setStaleFilter] = useState<boolean | null>(null);
  const [expanded, setExpanded] = useState<number | null>(null);

  useEffect(() => {
    const params = new URLSearchParams();
    if (topicFilter) params.set("topic", topicFilter);
    if (staleFilter !== null) params.set("is_stale", staleFilter ? "true" : "false");
    params.set("per_page", "100");

    Promise.allSettled([
      fetch(`/api/kb/facts?${params}`).then((r) => r.ok ? r.json() : null),
      fetch("/api/kb/stats").then((r) => r.ok ? r.json() : null),
    ]).then(([factsRes, statsRes]) => {
      if (factsRes.status === "fulfilled" && factsRes.value) setFacts(factsRes.value.facts);
      if (statsRes.status === "fulfilled" && statsRes.value) setStats(statsRes.value);
    }).finally(() => setLoading(false));
  }, [topicFilter, staleFilter]);

  return (
    <div>
      <div className="mb-8">
        <span className="text-xs font-mono tracking-wider text-[var(--accent-primary)] uppercase">
          Knowledge Base
        </span>
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--text-primary)] mt-1">
          Fact Explorer
        </h1>
        <p className="text-sm text-[var(--text-secondary)] mt-1">
          {stats ? `${stats.total_facts} facts (${stats.fresh_facts} fresh, ${stats.stale_facts} stale)` : "Browse all extracted economic insights"}
        </p>
      </div>

      {/* Facts by Topic Chart */}
      {stats?.facts_by_topic && stats.facts_by_topic.length > 0 && (
        <div className="glass-card p-5 mb-6">
          <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-4">
            Facts by Topic
          </h2>
          <div className="h-48">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={stats.facts_by_topic} layout="vertical" margin={{ left: 100 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                <XAxis type="number" tick={{ fontSize: 12, fill: "var(--text-secondary)" }} />
                <YAxis
                  type="category"
                  dataKey="topic"
                  tick={{ fontSize: 12, fill: "var(--text-secondary)" }}
                  width={95}
                />
                <Tooltip
                  contentStyle={{
                    background: "var(--bg-card)",
                    border: "1px solid var(--border)",
                    borderRadius: "0.5rem",
                    fontSize: "0.75rem",
                  }}
                  formatter={(v) => [`${Number(v)}`, "Facts"]}
                />
                <Bar dataKey="count" fill="var(--accent-primary)" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* Filters */}
      <div className="flex gap-3 mb-4">
        <select
          value={topicFilter ?? ""}
          onChange={(e) => setTopicFilter(e.target.value || null)}
          className="px-3 py-1.5 rounded-lg bg-[var(--bg-card)] border border-[var(--border)] text-xs text-[var(--text-secondary)]"
        >
          <option value="">All topics</option>
          {(stats?.facts_by_topic ?? []).map((t) => (
            <option key={t.topic} value={t.topic}>{t.topic} ({t.count})</option>
          ))}
        </select>
        <select
          value={staleFilter === null ? "" : staleFilter ? "stale" : "fresh"}
          onChange={(e) => {
            if (e.target.value === "") setStaleFilter(null);
            else setStaleFilter(e.target.value === "stale");
          }}
          className="px-3 py-1.5 rounded-lg bg-[var(--bg-card)] border border-[var(--border)] text-xs text-[var(--text-secondary)]"
        >
          <option value="">All status</option>
          <option value="fresh">Fresh only</option>
          <option value="stale">Stale only</option>
        </select>
      </div>

      {/* Facts Table */}
      <div className="glass-card p-5">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-[var(--border)]">
                <th className="text-left py-2 px-3 font-medium text-[var(--text-secondary)]">Claim</th>
                <th className="text-left py-2 px-3 font-medium text-[var(--text-secondary)]">Topic</th>
                <th className="text-left py-2 px-3 font-medium text-[var(--text-secondary)]">Country</th>
                <th className="text-right py-2 px-3 font-medium text-[var(--text-secondary)]">Confidence</th>
                <th className="text-center py-2 px-3 font-medium text-[var(--text-secondary)]">Status</th>
                <th className="text-right py-2 px-3 font-medium text-[var(--text-secondary)]">Date</th>
              </tr>
            </thead>
            <tbody className="text-[var(--text-primary)]">
              {facts.map((f) => (
                <tr
                  key={f.id}
                  className="border-b border-[var(--border)]/50 cursor-pointer hover:bg-[var(--bg-primary)]"
                  onClick={() => setExpanded(expanded === f.id ? null : f.id)}
                >
                  <td className="py-2 px-3 text-xs max-w-md">
                    <span className={expanded === f.id ? "" : "line-clamp-1"}>{f.claim}</span>
                  </td>
                  <td className="py-2 px-3 text-xs text-[var(--text-secondary)] capitalize">
                    {f.topic}{f.subtopic ? ` / ${f.subtopic}` : ""}
                  </td>
                  <td className="py-2 px-3 font-mono text-xs text-[var(--text-muted)]">{f.country_iso3 ?? "--"}</td>
                  <td className="text-right py-2 px-3">
                    <span className={`font-mono text-xs ${
                      f.confidence > 0.7 ? "text-emerald-600" : f.confidence > 0.4 ? "text-amber-600" : "text-red-500"
                    }`}>
                      {(f.confidence * 100).toFixed(0)}%
                    </span>
                  </td>
                  <td className="text-center py-2 px-3">
                    <span className={`inline-flex w-2 h-2 rounded-full ${f.is_stale ? "bg-red-500" : "bg-emerald-500"}`} />
                  </td>
                  <td className="text-right py-2 px-3 text-xs text-[var(--text-muted)] whitespace-nowrap">
                    {new Date(f.created_at).toLocaleDateString()}
                  </td>
                </tr>
              ))}
              {facts.length === 0 && (
                <tr>
                  <td colSpan={6} className="py-8 text-center text-sm text-[var(--text-muted)]">
                    {loading ? "Loading..." : "No facts found. Run the compiler to extract insights from analysis data."}
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Build to verify no TypeScript errors**

Run: `cd /Users/mddeluairhossen/equilibria/web && npm run build`
Expected: Build succeeds

- [ ] **Step 3: Commit**

```bash
git add web/app/knowledge/facts/page.tsx
git commit -m "add fact explorer page with filters and topic chart"
```

---

### Task 12: Frontend — Sidebar + Dashboard Integration

**Files:**
- Modify: `web/app/Sidebar.tsx`
- Modify: `web/app/page.tsx`

- [ ] **Step 1: Add KB to sidebar**

In `web/app/Sidebar.tsx`, add a new nav section after the "Tools" section (after the AI Chat entry). In the `navSections` array, add a new section:

```tsx
  {
    label: "Intelligence",
    items: [
      { name: "Knowledge Base", href: "/knowledge", icon: "KB" },
      { name: "Fact Explorer", href: "/knowledge/facts", icon: "FE" },
    ],
  },
```

Insert this between the "Tools" section and the "Reference" section.

- [ ] **Step 2: Add KB stats card to dashboard**

In `web/app/page.tsx`, add state for KB stats and a card. After the existing state declarations add:

```tsx
  const [kbStats, setKbStats] = useState<{ total_facts: number; total_articles: number; last_compile: string | null } | null>(null);
```

In the `fetchData` function, add after the briefings fetch:

```tsx
          const kbRes = await fetch("/api/kb/stats");
          if (kbRes.ok) setKbStats(await kbRes.json());
```

Then add a KB card in the grid section, after the "Data Sources" card:

```tsx
        {/* Knowledge Base */}
        <div className="glass-card p-5">
          <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-4">
            Knowledge Base
          </h2>
          {kbStats ? (
            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <span className="text-sm text-[var(--text-secondary)]">Articles</span>
                <span className="text-sm font-mono font-semibold text-[var(--text-primary)]">{kbStats.total_articles}</span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-sm text-[var(--text-secondary)]">Facts</span>
                <span className="text-sm font-mono font-semibold text-[var(--text-primary)]">{kbStats.total_facts}</span>
              </div>
              {kbStats.last_compile && (
                <p className="text-xs text-[var(--text-muted)]">
                  Last compiled {new Date(kbStats.last_compile).toLocaleDateString()}
                </p>
              )}
            </div>
          ) : (
            <p className="text-sm text-[var(--text-muted)]">{loading ? "Loading..." : "Not yet compiled"}</p>
          )}
        </div>
```

- [ ] **Step 3: Build to verify no TypeScript errors**

Run: `cd /Users/mddeluairhossen/equilibria/web && npm run build`
Expected: Build succeeds

- [ ] **Step 4: Commit**

```bash
git add web/app/Sidebar.tsx web/app/page.tsx
git commit -m "add KB to sidebar navigation and dashboard stats card"
```

---

### Task 13: Update CLAUDE.md + Run Full Test Suite

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Update CLAUDE.md**

Update the following in `CLAUDE.md`:

- DB schema description: change "8 tables" to "12 tables" (add: kb_facts, kb_articles, kb_article_facts, kb_sources)
- AI Brain section: change "22 tools" to "24 tools", add search_knowledge and file_insight to the tool list
- Add a new section:

```markdown
## Knowledge Base
LLM-compiled wiki from accumulated analysis data. Three-stage compiler pipeline:
1. Fact Extraction: Claude extracts claims from analysis_results and briefings
2. Article Compilation: Groups of 3+ facts compiled into markdown articles
3. Staleness Sweep: 30-day TTL with source-linked refresh

Tables: kb_facts (claims with confidence/evidence), kb_articles (compiled markdown), kb_article_facts (links), kb_sources (data lineage). FTS5 search via kb_search virtual table.

Frontend: /knowledge (index), /knowledge/[slug] (article), /knowledge/facts (explorer).
```

- [ ] **Step 2: Run full test suite**

Run: `cd /Users/mddeluairhossen/equilibria && uv run pytest -x -q`
Expected: All tests PASS (existing + new KB tests)

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "update CLAUDE.md: 12 tables, 24 tools, KB docs"
```

---

## Self-Review

**1. Spec coverage:**
- Data model (4 tables + FTS5 + indexes): Task 1 ✅
- FTS5 search helpers: Task 2 ✅
- Fact extraction (Stage 1): Task 3 ✅
- Article compilation (Stage 2): Task 4 ✅
- Staleness sweep (Stage 3) + orchestrator: Task 5 ✅
- Brain tools (search_knowledge, file_insight): Task 6 ✅
- System prompt update: Task 6 ✅
- REST API (6 endpoints): Task 7 ✅
- Post-collect hook: Task 8 ✅
- Post-generate hook: Task 8 ✅
- Frontend /knowledge index: Task 9 ✅
- Frontend /knowledge/[slug] detail: Task 10 ✅
- Frontend /knowledge/facts explorer: Task 11 ✅
- Sidebar + dashboard integration: Task 12 ✅
- CLAUDE.md update: Task 13 ✅

**2. Placeholder scan:** No TBD, TODO, or "add appropriate" placeholders found.

**3. Type consistency:**
- `store_fact()` signature consistent between Task 3 definition and Task 6 usage
- `search_kb()` signature consistent between Task 2 definition and Tasks 6, 7 usage
- `compile_kb()` signature consistent between Task 5 definition and Task 7 usage
- `index_fact()` / `index_article()` consistent between Task 2 definition and Tasks 3, 4 usage
- `extract_new_facts()` consistent between Task 3 definition and Tasks 5, 8 usage
- Frontend API paths match backend routes (/api/kb/*)
