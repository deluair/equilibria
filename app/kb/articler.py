"""Stage 2 compiler: assemble KB facts into articles via Claude."""

import json
import logging
import re

from app.config import settings
from app.db import execute, fetch_all, get_db, release_db
from app.kb.search import index_article

logger = logging.getLogger(__name__)

COMPILE_PROMPT = """You are an economics article compiler. Given the following factual claims
about a specific topic and country, write a concise markdown article that synthesizes them.

Return ONLY a JSON object with:
- "title": article title (clear, descriptive)
- "slug": URL-friendly lowercase slug with hyphens (e.g. "bangladesh-trade-overview")
- "summary": 1-2 sentence summary
- "content": markdown article body synthesizing all facts

Topic: {topic}
Country: {country_iso3}

Facts:
{facts_text}"""

MAX_FACTS_PER_ARTICLE = 20


async def get_compilable_groups() -> list[dict]:
    """Query kb_facts grouped by (topic, country_iso3) having 3+ non-stale facts."""
    rows = await fetch_all(
        "SELECT topic, country_iso3, COUNT(*) as count "
        "FROM kb_facts WHERE is_stale = 0 "
        "GROUP BY topic, country_iso3 "
        "HAVING COUNT(*) >= 3 "
        "ORDER BY count DESC"
    )
    return rows


def _sanitize_slug(slug: str) -> str:
    """Sanitize a slug to be URL-friendly lowercase with hyphens."""
    slug = slug.lower().strip()
    slug = re.sub(r"[^a-z0-9-]", "-", slug)
    slug = re.sub(r"-+", "-", slug)
    slug = slug.strip("-")
    return slug


def _parse_json_object(text: str) -> dict | None:
    """Extract a JSON object from Claude's response text."""
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        return json.loads(text[start : end + 1])
    except json.JSONDecodeError:
        return None


async def _call_claude(topic: str, country_iso3: str | None, facts: list[dict]) -> dict | None:
    """Call Claude to compile facts into an article. Returns parsed dict or None."""
    if not settings.anthropic_api_key:
        return None

    facts_text = "\n".join(
        f"- [{f['confidence']:.0%} confidence] {f['claim']}" for f in facts[:MAX_FACTS_PER_ARTICLE]
    )

    try:
        import anthropic

        client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            messages=[
                {
                    "role": "user",
                    "content": COMPILE_PROMPT.format(
                        topic=topic,
                        country_iso3=country_iso3 or "Global",
                        facts_text=facts_text,
                    ),
                }
            ],
        )
        return _parse_json_object(response.content[0].text)
    except Exception:
        logger.exception("Claude article compilation failed for %s/%s", topic, country_iso3)
        return None


async def compile_article_for_group(topic: str, country_iso3: str | None) -> int | None:
    """Compile an article for a single (topic, country_iso3) group.

    Returns article_id on success, None on failure or missing API key.
    """
    if not settings.anthropic_api_key:
        return None

    # Fetch non-stale facts for this group
    if country_iso3:
        facts = await fetch_all(
            "SELECT id, claim, confidence FROM kb_facts "
            "WHERE topic = ? AND country_iso3 = ? AND is_stale = 0 "
            "ORDER BY confidence DESC LIMIT ?",
            (topic, country_iso3, MAX_FACTS_PER_ARTICLE),
        )
    else:
        facts = await fetch_all(
            "SELECT id, claim, confidence FROM kb_facts "
            "WHERE topic = ? AND country_iso3 IS NULL AND is_stale = 0 "
            "ORDER BY confidence DESC LIMIT ?",
            (topic, MAX_FACTS_PER_ARTICLE),
        )

    if len(facts) < 3:
        return None

    result = await _call_claude(topic, country_iso3, facts)
    if not result or not result.get("content"):
        return None

    title = result.get("title", f"{topic} - {country_iso3 or 'Global'}")
    slug = _sanitize_slug(result.get("slug", f"{topic}-{country_iso3 or 'global'}"))
    summary = result.get("summary", "")
    content = result["content"]
    fact_count = len(facts)
    fact_ids = [f["id"] for f in facts]

    # Use a single connection for insert + last_insert_rowid or update + re-link
    db = await get_db()
    try:
        # Check if article already exists for this group
        if country_iso3:
            existing = await db.fetch_one(
                "SELECT id FROM kb_articles WHERE topic = ? AND country_iso3 = ?",
                (topic, country_iso3),
            )
        else:
            existing = await db.fetch_one(
                "SELECT id FROM kb_articles WHERE topic = ? AND country_iso3 IS NULL",
                (topic,),
            )

        if existing:
            article_id = existing["id"]
            await db.conn.execute(
                "UPDATE kb_articles SET slug = ?, title = ?, content = ?, summary = ?, "
                "fact_count = ?, updated_at = datetime('now') WHERE id = ?",
                (slug, title, content, summary, fact_count, article_id),
            )
            # Clear old fact links and re-link
            await db.conn.execute(
                "DELETE FROM kb_article_facts WHERE article_id = ?",
                (article_id,),
            )
            for fid in fact_ids:
                await db.conn.execute(
                    "INSERT INTO kb_article_facts (article_id, fact_id) VALUES (?, ?)",
                    (article_id, fid),
                )
            await db.conn.commit()
        else:
            await db.conn.execute(
                "INSERT INTO kb_articles (slug, title, topic, country_iso3, content, summary, fact_count) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (slug, title, topic, country_iso3, content, summary, fact_count),
            )
            cursor = await db.conn.execute("SELECT last_insert_rowid() as id")
            row = await cursor.fetchone()
            article_id = row[0]
            for fid in fact_ids:
                await db.conn.execute(
                    "INSERT INTO kb_article_facts (article_id, fact_id) VALUES (?, ?)",
                    (article_id, fid),
                )
            await db.conn.commit()
    finally:
        await release_db(db)

    # FTS5 index
    await index_article(article_id, title, content)

    logger.info("Compiled article %d: %s (%s/%s)", article_id, title, topic, country_iso3)
    return article_id


async def compile_articles() -> int:
    """Compile articles for all compilable groups. Returns count of articles compiled."""
    groups = await get_compilable_groups()
    count = 0
    for group in groups:
        article_id = await compile_article_for_group(group["topic"], group["country_iso3"])
        if article_id is not None:
            count += 1
    return count
