# Equilibria Knowledge Base Design

## Overview

An LLM-compiled knowledge base that automatically accumulates economic insights from Equilibria's 18 analytical layers, 13 data collectors, 7 briefings, and AI conversations. Inspired by Karpathy's "LLM Knowledge Bases" concept: raw data is collected, compiled by an LLM into structured facts and markdown articles, then queryable by the AI brain and viewable on the frontend.

**Goal:** Every analysis run, collector sweep, briefing generation, and chat conversation contributes to a growing, self-maintaining economics wiki. The AI brain searches this knowledge before running expensive computations, and files new insights as it discovers them.

**Architecture:** SQLite-native with FTS5 full-text search. Four new tables in equilibria.db. Three-stage async compiler pipeline (extract facts, compile articles, sweep staleness). Two new AI tools. Three new frontend pages. Zero new dependencies.

---

## Data Model

Four new tables in `equilibria.db`:

### `kb_facts`

Individual claims/insights with evidence chain.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment |
| claim | TEXT NOT NULL | The insight itself, e.g. "Bangladesh RCA in textiles is 12.3, 4x the regional average" |
| topic | TEXT NOT NULL | Layer-aligned category: trade, macro, labor, development, agricultural, integration, financial, health, environmental, public, spatial, political, behavioral, industrial, monetary, energy, demographic, methods |
| subtopic | TEXT | More specific: rca, gravity, phillips_curve, etc. |
| country_iso3 | TEXT | NULL for global insights, FK to countries |
| confidence | REAL | 0.0-1.0, derived from source quality and recency |
| evidence | TEXT | JSON array: `[{type, id, summary}]` where type is "analysis_result", "data_point", "briefing", or "conversation" |
| created_at | TEXT | DEFAULT datetime('now') |
| updated_at | TEXT | DEFAULT datetime('now') |
| stale_at | TEXT | DEFAULT datetime('now', '+30 days') |
| is_stale | INTEGER | DEFAULT 0 |

### `kb_articles`

Compiled markdown articles (the wiki view).

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment |
| slug | TEXT UNIQUE | URL-friendly key, e.g. "bangladesh-trade-competitiveness" |
| title | TEXT NOT NULL | Article title |
| topic | TEXT NOT NULL | Same topic taxonomy as kb_facts |
| country_iso3 | TEXT | NULL for cross-country articles |
| content | TEXT NOT NULL | Markdown body with inline fact citations |
| summary | TEXT NOT NULL | 2-3 sentence abstract for index/search |
| fact_count | INTEGER | Number of facts this article synthesizes |
| created_at | TEXT | DEFAULT datetime('now') |
| updated_at | TEXT | DEFAULT datetime('now') |

### `kb_article_facts`

Many-to-many: articles to their source facts.

| Column | Type | Description |
|--------|------|-------------|
| article_id | INTEGER FK | References kb_articles(id) |
| fact_id | INTEGER FK | References kb_facts(id) |

Primary key on (article_id, fact_id).

### `kb_sources`

Data lineage tracking for staleness detection.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment |
| fact_id | INTEGER FK | References kb_facts(id) |
| source_type | TEXT NOT NULL | "data_point", "analysis_result", "briefing", "conversation_message" |
| source_id | INTEGER NOT NULL | PK of the source row in its table |
| source_date | TEXT NOT NULL | When the source data was created |

### FTS5 Virtual Table

```sql
CREATE VIRTUAL TABLE IF NOT EXISTS kb_search USING fts5(
    claim, title, content, summary,
    content='' -- external content mode, populated manually
);
```

Populated via explicit INSERT into kb_search when facts/articles are created or updated (in extractor.py and articler.py). Each row gets a rowid matching the source fact or article id (facts use positive ids, articles use negative ids to avoid collision). Searched via `kb_search MATCH ?` with BM25 ranking. Deletions handled via DELETE from kb_search WHERE rowid = ?.

### Indexes

- `kb_facts(topic, country_iso3)`
- `kb_facts(is_stale)`
- `kb_facts(subtopic)`
- `kb_articles(slug)` -- already UNIQUE
- `kb_articles(topic, country_iso3)`
- `kb_sources(source_type, source_id)`
- `kb_sources(fact_id)`

---

## Compiler Pipeline

Lives in `app/kb/`. Three stages, all async.

### Stage 1: Fact Extraction (`app/kb/extractor.py`)

**Trigger:** After collector runs, after briefing generation, or manual via `/api/kb/compile`.

**Process:**
1. Query `analysis_results` for rows with `created_at` > last compile timestamp (stored in `collection_log` with source="kb_compiler")
2. Query `briefings` for rows with `created_at` > last compile timestamp
3. Query `conversation_messages` for rows with `created_at` > last compile timestamp
4. For each batch of source rows, call Claude with a structured extraction prompt:
   - Input: source data (analysis result JSON, briefing content, conversation text)
   - Output: JSON array of `{claim, topic, subtopic, country_iso3, confidence}`
   - Prompt instructs: extract only factual, verifiable claims. No opinions. No hedged language. Confidence reflects data quality.
5. Deduplicate: for each extracted claim, FTS5 search existing facts with same topic + country. If match score > threshold, ask Claude: "Is this a duplicate or update of fact #{id}?" If duplicate, skip. If update, update existing fact's claim text, bump updated_at, refresh stale_at.
6. Insert new facts into `kb_facts`, link sources via `kb_sources`

**Cost control:** Batch sources into groups of 5-10 per Claude call. Use structured JSON output mode. Typical extraction: ~5-15 API calls per compile run.

### Stage 2: Article Compilation (`app/kb/articler.py`)

**Trigger:** Runs after Stage 1 completes.

**Process:**
1. Group all non-stale facts by (topic, country_iso3)
2. For each group with 3+ facts:
   - Check if article already exists for this (topic, country_iso3)
   - If exists and no new facts since last update: skip
   - If exists with new facts: update article
   - If no article and 3+ facts: create new article
3. Claude prompt for article creation/update:
   - Input: all facts for this group, existing article content (if updating)
   - Output: `{title, slug, summary, content}` where content is markdown with inline `[Fact #N]` citations
4. Insert/update `kb_articles`, link via `kb_article_facts`

**Cost control:** Only compile articles for groups with changes. Typical run: ~3-10 API calls.

### Stage 3: Staleness Sweep (`app/kb/staleness.py`)

**Trigger:** Runs after Stage 2, or independently on schedule.

**Process:**
1. For each fact with `is_stale = 0`:
   - Query `kb_sources` for linked sources
   - For each source, check if the source row has been updated (e.g., a data_point with newer date, an analysis_result recomputed)
   - If source updated: set `stale_at = datetime('now', '+30 days')`, bump confidence by 0.05 (cap at 1.0), update `updated_at`
   - If `stale_at < datetime('now')` and no source update: set `is_stale = 1`, reduce confidence by 0.1 (floor at 0.1)
2. For each article: count stale vs total facts. If >50% stale, mark for recompilation (Stage 2 will pick it up next run)

### Compiler Orchestrator (`app/kb/compiler.py`)

```python
async def compile_kb(db, full: bool = False) -> dict:
    """Run all three stages. If full=True, reprocess all sources, not just new ones."""
    extracted = await extract_facts(db, full=full)
    compiled = await compile_articles(db)
    swept = await sweep_staleness(db)
    # Log compile run
    await log_collection(db, source="kb_compiler", series_count=compiled, point_count=extracted)
    return {facts_extracted: extracted, articles_compiled: compiled, facts_swept: swept}
```

---

## Brain Integration

### New Tool: `search_knowledge`

Added to `TOOL_REGISTRY` in `app/ai/tools.py`.

**Schema:**
```json
{
  "name": "search_knowledge",
  "description": "Search the accumulated knowledge base for economic insights and articles. Use this BEFORE running analysis tools to check if the answer already exists.",
  "input_schema": {
    "type": "object",
    "properties": {
      "query": {"type": "string", "description": "Search query"},
      "topic": {"type": "string", "description": "Filter by topic (trade, macro, labor, etc.)"},
      "country_iso3": {"type": "string", "description": "Filter by country ISO3 code"},
      "include_stale": {"type": "boolean", "default": false, "description": "Include stale facts"}
    },
    "required": ["query"]
  }
}
```

**Implementation:** FTS5 MATCH query on `kb_search`, filtered by topic/country/staleness. Returns top 10 facts + top 5 matching articles with summaries.

### New Tool: `file_insight`

**Schema:**
```json
{
  "name": "file_insight",
  "description": "File a new economic insight discovered during analysis. Use when your analysis produces a novel finding worth preserving.",
  "input_schema": {
    "type": "object",
    "properties": {
      "claim": {"type": "string", "description": "The factual claim"},
      "topic": {"type": "string", "description": "Topic category"},
      "subtopic": {"type": "string", "description": "Specific subtopic"},
      "country_iso3": {"type": "string", "description": "Country code, null for global"},
      "evidence": {
        "type": "array",
        "items": {
          "type": "object",
          "properties": {
            "type": {"type": "string"},
            "id": {"type": "integer"},
            "summary": {"type": "string"}
          }
        }
      }
    },
    "required": ["claim", "topic", "evidence"]
  }
}
```

**Implementation:** Dedup check via FTS5, then insert into `kb_facts` + `kb_sources`. Returns `{fact_id, status: "created"|"duplicate"}`.

### System Prompt Update

Append to existing brain system prompt in `app/ai/brain.py`:

```
You have access to a knowledge base of accumulated economic insights.
- Before running analysis tools, search the knowledge base with search_knowledge.
- If you find a recent, high-confidence match (confidence > 0.7), use it and cite "KB fact #{id}".
- When your analysis produces a novel finding, file it with file_insight.
- Prefer fresh facts over stale ones. Note staleness if citing older facts.
```

Tool count: 22 -> 24.

---

## API Endpoints

New file: `app/api/kb.py`

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/kb/articles` | Paginated article list. Query params: `topic`, `country_iso3`, `page`, `per_page` |
| GET | `/api/kb/articles/{slug}` | Single article by slug |
| GET | `/api/kb/facts` | Paginated fact list. Query params: `topic`, `country_iso3`, `is_stale`, `page`, `per_page` |
| GET | `/api/kb/search` | FTS5 search. Query params: `q`, `topic`, `country_iso3`. Returns facts + articles |
| GET | `/api/kb/stats` | KB statistics: total facts, total articles, fresh/stale ratio, last compile time, facts by topic |
| POST | `/api/kb/compile` | Trigger compiler. Query param: `full=true` for full reprocess |

All endpoints follow existing FastAPI patterns in the project. JSON responses, standard error handling.

---

## Frontend

### `/knowledge` (Index Page) -- `web/app/knowledge/page.tsx`

- Header: "Knowledge Base" with Layer badge style, subtitle with live stats (N articles, M facts, last compiled)
- Search bar hitting `/api/kb/search`
- Topic filter chips (one per layer, same color scheme)
- Country filter dropdown
- Article cards in responsive grid: title, summary, fact count, topic badge, last updated
- Sorted by most recently updated
- "Awaiting data" state when KB is empty (same pattern as other layer pages)

### `/knowledge/[slug]` (Article Detail) -- `web/app/knowledge/[slug]/page.tsx`

- Rendered markdown body (use react-markdown or similar, already in the ecosystem)
- Right sidebar: related facts with confidence badges (green >0.7, amber 0.4-0.7, red <0.4)
- Source links: clicking a fact shows its evidence chain (links to analysis results, briefings)
- "Last compiled" timestamp
- Staleness banner if >50% facts are stale
- Back link to index

### `/knowledge/facts` (Fact Explorer) -- `web/app/knowledge/facts/page.tsx`

- Table view: claim (truncated), topic, country, confidence bar, stale badge, created date
- Sortable columns, filterable by topic/country/staleness
- Click row to expand: full claim text, evidence chain with links
- Stats bar at top: total facts, fresh vs stale donut, facts-by-topic horizontal bar

### Dashboard Integration

- `web/app/page.tsx`: new card in the layer grid: "Knowledge Base" with article count, fact count, last compile time
- `web/app/Sidebar.tsx`: new nav entry `{ name: "Knowledge Base", href: "/knowledge", icon: "KB" }`

### Styling

Follows existing patterns: glass-card, CSS variables, font-mono for data, Recharts for any charts. No new styling dependencies.

---

## File Structure

### New Files

```
app/
  kb/
    __init__.py
    compiler.py        # Orchestrator: compile_kb()
    extractor.py       # Stage 1: fact extraction
    articler.py        # Stage 2: article compilation
    staleness.py       # Stage 3: staleness sweep
    search.py          # FTS5 search helpers
  api/
    kb.py              # 6 REST endpoints
web/
  app/
    knowledge/
      page.tsx         # Index with search + article grid
      [slug]/
        page.tsx       # Article detail
      facts/
        page.tsx       # Fact explorer table
```

### Modified Files

- `app/db.py` -- add 4 tables + FTS5 virtual table + indexes to SCHEMA
- `app/ai/brain.py` -- append KB instructions to system prompt
- `app/ai/tools.py` -- add search_knowledge and file_insight to TOOL_REGISTRY
- `app/collectors/base.py` -- add post-collect hook calling `extract_facts()`
- `app/briefings/base.py` -- add post-generate hook calling `extract_facts()`
- `web/app/Sidebar.tsx` -- add Knowledge Base nav entry
- `web/app/page.tsx` -- add KB stats card to dashboard
- `CLAUDE.md` -- update module counts, table count (8->12), tool count (22->24)

---

## Staleness Policy

- **Default TTL:** 30 days. Facts older than 30 days without source updates marked stale.
- **Source-linked refresh:** When a collector updates data_points that a fact references (via kb_sources), the fact's stale_at resets to now + 30 days and confidence gets a 0.05 bump.
- **Confidence decay:** Stale facts lose 0.1 confidence per sweep cycle (floor 0.1). Fresh source updates restore 0.05 (cap 1.0).
- **Article staleness:** Articles with >50% stale facts get flagged for recompilation. The compiler rewrites them on next run.
- **No deletion:** Stale facts are never deleted, only deprioritized. They remain searchable with `include_stale=true`.

---

## Cost Estimate

Compiler uses Claude for extraction and article compilation:
- Fact extraction: ~5-15 calls per compile (batched sources)
- Article compilation: ~3-10 calls per compile (only changed groups)
- Deduplication checks: ~2-5 calls per compile
- Total per compile run: ~10-30 Claude calls, ~50K-150K tokens
- Brain tools (search_knowledge, file_insight): zero LLM cost, pure SQLite queries

---

## Non-Goals (Explicit)

- No vector/embedding search in v1 (FTS5 keyword search only)
- No manual article editing UI (articles are LLM-compiled only)
- No real-time streaming compilation (batch only)
- No cross-project knowledge sharing
- No authentication/access control on KB endpoints
