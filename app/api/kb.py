"""Knowledge Base API routes."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.db import fetch_all, fetch_one
from app.kb.compiler import compile_kb
from app.kb.search import search_kb

router = APIRouter(prefix="/kb", tags=["knowledge-base"])


@router.get("/stats")
async def kb_stats():
    """KB statistics: totals, stale/fresh counts, last compile, facts by topic."""
    total_facts_row = await fetch_one("SELECT COUNT(*) AS cnt FROM kb_facts")
    total_articles_row = await fetch_one("SELECT COUNT(*) AS cnt FROM kb_articles")
    stale_row = await fetch_one("SELECT COUNT(*) AS cnt FROM kb_facts WHERE is_stale = 1")
    fresh_row = await fetch_one("SELECT COUNT(*) AS cnt FROM kb_facts WHERE is_stale = 0")

    last_compile_row = await fetch_one(
        "SELECT finished_at FROM collection_log WHERE source = 'kb_compiler' "
        "ORDER BY id DESC LIMIT 1"
    )

    facts_by_topic = await fetch_all(
        "SELECT topic, COUNT(*) AS count FROM kb_facts GROUP BY topic ORDER BY count DESC"
    )

    return {
        "total_facts": total_facts_row["cnt"] if total_facts_row else 0,
        "total_articles": total_articles_row["cnt"] if total_articles_row else 0,
        "stale_count": stale_row["cnt"] if stale_row else 0,
        "fresh_count": fresh_row["cnt"] if fresh_row else 0,
        "last_compile": last_compile_row["finished_at"] if last_compile_row else None,
        "facts_by_topic": facts_by_topic,
    }


@router.get("/articles")
async def kb_articles(
    topic: str | None = None,
    country_iso3: str | None = None,
    page: int = 1,
    per_page: int = 20,
):
    """Paginated article list with optional topic/country filters."""
    conditions: list[str] = []
    params: list = []

    if topic is not None:
        conditions.append("topic = ?")
        params.append(topic)
    if country_iso3 is not None:
        conditions.append("country_iso3 = ?")
        params.append(country_iso3)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    offset = (page - 1) * per_page

    count_row = await fetch_one(f"SELECT COUNT(*) AS cnt FROM kb_articles{where}", tuple(params))
    total = count_row["cnt"] if count_row else 0

    rows = await fetch_all(
        f"SELECT * FROM kb_articles{where} ORDER BY updated_at DESC LIMIT ? OFFSET ?",
        tuple(params) + (per_page, offset),
    )

    return {"articles": rows, "total": total, "page": page, "per_page": per_page}


@router.get("/articles/{slug}")
async def kb_article_by_slug(slug: str):
    """Single article by slug, with linked facts."""
    article = await fetch_one("SELECT * FROM kb_articles WHERE slug = ?", (slug,))
    if article is None:
        raise HTTPException(status_code=404, detail="Article not found")

    facts = await fetch_all(
        "SELECT f.* FROM kb_facts f "
        "JOIN kb_article_facts af ON af.fact_id = f.id "
        "WHERE af.article_id = ?",
        (article["id"],),
    )

    return {**article, "facts": facts}


@router.get("/facts")
async def kb_facts(
    topic: str | None = None,
    country_iso3: str | None = None,
    is_stale: bool | None = None,
    page: int = 1,
    per_page: int = 20,
):
    """Paginated fact list with optional filters."""
    conditions: list[str] = []
    params: list = []

    if topic is not None:
        conditions.append("topic = ?")
        params.append(topic)
    if country_iso3 is not None:
        conditions.append("country_iso3 = ?")
        params.append(country_iso3)
    if is_stale is not None:
        conditions.append("is_stale = ?")
        params.append(1 if is_stale else 0)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    offset = (page - 1) * per_page

    count_row = await fetch_one(f"SELECT COUNT(*) AS cnt FROM kb_facts{where}", tuple(params))
    total = count_row["cnt"] if count_row else 0

    rows = await fetch_all(
        f"SELECT * FROM kb_facts{where} ORDER BY updated_at DESC LIMIT ? OFFSET ?",
        tuple(params) + (per_page, offset),
    )

    return {"facts": rows, "total": total, "page": page, "per_page": per_page}


@router.get("/search")
async def kb_search_endpoint(
    q: str,
    topic: str | None = None,
    country_iso3: str | None = None,
):
    """FTS5 search across facts and articles."""
    result = await search_kb(query=q, topic=topic, country_iso3=country_iso3)
    return result


@router.post("/compile")
async def kb_compile(full: bool = False):
    """Trigger KB compilation pipeline."""
    result = await compile_kb(full=full)
    return result
