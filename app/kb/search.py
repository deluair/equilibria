"""FTS5 search helpers for the knowledge base."""

from app.db import execute, fetch_all


async def index_fact(fact_id: int, claim: str) -> None:
    """Index a fact into FTS5. Uses positive rowid."""
    await execute(
        "INSERT OR REPLACE INTO kb_search (rowid, fact_id, article_id, title, content) "
        "VALUES (?, ?, ?, ?, ?)",
        (fact_id, str(fact_id), "", "", claim),
    )


async def index_article(article_id: int, title: str, content: str) -> None:
    """Index an article into FTS5. Uses negative rowid to avoid collision with facts."""
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
    """Search the knowledge base via FTS5 MATCH.

    Returns {"facts": [...], "articles": [...]}.
    """
    # FTS5 match
    rows = await fetch_all(
        "SELECT rowid, fact_id, article_id, title, content, "
        "rank FROM kb_search WHERE kb_search MATCH ? ORDER BY rank LIMIT ?",
        (query, limit * 2),
    )

    facts = []
    articles = []

    for row in rows:
        fact_id_str = row["fact_id"]
        article_id_str = row["article_id"]

        if fact_id_str and fact_id_str != "":
            fact_id = int(fact_id_str)
            # Fetch full fact row with optional filters
            sql = "SELECT * FROM kb_facts WHERE id = ?"
            params: list = [fact_id]
            if topic is not None:
                sql += " AND topic = ?"
                params.append(topic)
            if country_iso3 is not None:
                sql += " AND country_iso3 = ?"
                params.append(country_iso3)
            if not include_stale:
                sql += " AND is_stale = 0"
            fact_rows = await fetch_all(sql, tuple(params))
            for f in fact_rows:
                if not any(existing["fact_id"] == f["id"] for existing in facts):
                    facts.append({"fact_id": f["id"], **f})

        if article_id_str and article_id_str != "":
            article_id = int(article_id_str)
            sql = "SELECT * FROM kb_articles WHERE id = ?"
            params = [article_id]
            if topic is not None:
                sql += " AND topic = ?"
                params.append(topic)
            if country_iso3 is not None:
                sql += " AND country_iso3 = ?"
                params.append(country_iso3)
            article_rows = await fetch_all(sql, tuple(params))
            for a in article_rows:
                if not any(existing["article_id"] == a["id"] for existing in articles):
                    articles.append({"article_id": a["id"], **a})

    return {"facts": facts[:limit], "articles": articles[:limit]}
