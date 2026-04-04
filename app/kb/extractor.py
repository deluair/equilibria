"""Stage 1 compiler: extract factual claims from analysis results and briefings."""

import json
import logging

from app.config import settings
from app.db import execute, fetch_all, fetch_one, get_db, release_db
from app.kb.search import index_fact

logger = logging.getLogger(__name__)

LAYER_TO_TOPIC = {
    "l1": "trade",
    "l2": "macro",
    "l3": "labor",
    "l4": "development",
    "l5": "agricultural",
    "l6": "integration",
    "l7": "financial",
    "l8": "health",
    "l9": "environmental",
    "l10": "public",
    "l11": "spatial",
    "l12": "political",
    "l13": "behavioral",
    "l14": "industrial",
    "l15": "monetary",
    "l16": "energy",
    "l17": "demographic",
    "l18": "methods",
}

EXTRACT_PROMPT = """You are an economics fact extractor. Given the following analysis output,
extract verifiable factual claims. Return ONLY a JSON array of objects, each with:
- "claim": a single factual statement (one sentence)
- "topic": economics topic (trade, macro, labor, development, agricultural, etc.)
- "subtopic": specific subtopic if identifiable (e.g. "rca", "gdp_decomposition")
- "country_iso3": 3-letter ISO code if country-specific, else null
- "confidence": 0.0-1.0 how confident the claim is based on evidence strength

Extract 1-5 claims. Focus on quantitative, verifiable statements.
If the input has no extractable facts, return an empty array [].

Input:
{text}"""


async def store_fact(
    claim: str,
    topic: str,
    confidence: float,
    evidence: list,
    source_type: str,
    source_id: int,
    subtopic: str | None = None,
    country_iso3: str | None = None,
) -> int:
    """Insert a fact into kb_facts, link via kb_sources, index in FTS5. Returns fact_id."""
    # Use a single connection so last_insert_rowid() is reliable
    db = await get_db()
    try:
        await db.conn.execute(
            "INSERT INTO kb_facts (claim, topic, subtopic, country_iso3, confidence, evidence) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (claim, topic, subtopic, country_iso3, confidence, json.dumps(evidence)),
        )
        cursor = await db.conn.execute("SELECT last_insert_rowid() as id")
        row = await cursor.fetchone()
        fact_id = row[0]

        await db.conn.execute(
            "INSERT INTO kb_sources (fact_id, source_type, source_id, source_date) "
            "VALUES (?, ?, ?, datetime('now'))",
            (fact_id, source_type, source_id),
        )
        await db.conn.commit()
    finally:
        await release_db(db)

    # FTS5 index
    await index_fact(fact_id, claim)

    return fact_id


def _parse_json_array(text: str) -> list[dict]:
    """Extract a JSON array from Claude's response text."""
    start = text.find("[")
    end = text.rfind("]")
    if start == -1 or end == -1 or end <= start:
        return []
    try:
        return json.loads(text[start : end + 1])
    except json.JSONDecodeError:
        return []


def _build_text_from_result(result: dict) -> str:
    """Build a truncated text blob from an analysis result dict."""
    parts = []
    if result.get("analysis_type"):
        parts.append(f"Analysis: {result['analysis_type']}")
    if result.get("country_iso3"):
        parts.append(f"Country: {result['country_iso3']}")
    if result.get("layer"):
        parts.append(f"Layer: {result['layer']}")
    if result.get("score") is not None:
        parts.append(f"Score: {result['score']}")
    if result.get("result"):
        r = result["result"]
        if isinstance(r, str):
            parts.append(f"Result: {r[:3000]}")
        else:
            parts.append(f"Result: {json.dumps(r)[:3000]}")
    return "\n".join(parts)[:4000]


def _build_text_from_briefing(briefing: dict) -> str:
    """Build a truncated text blob from a briefing dict."""
    parts = []
    if briefing.get("title"):
        parts.append(f"Title: {briefing['title']}")
    if briefing.get("country_iso3"):
        parts.append(f"Country: {briefing['country_iso3']}")
    if briefing.get("composite_score") is not None:
        parts.append(f"Composite Score: {briefing['composite_score']}")
    if briefing.get("signal"):
        parts.append(f"Signal: {briefing['signal']}")
    if briefing.get("content"):
        parts.append(f"Content: {briefing['content'][:3000]}")
    return "\n".join(parts)[:4000]


async def _call_claude(text: str) -> list[dict]:
    """Call Claude to extract facts from text. Returns parsed list of claim dicts."""
    if not settings.anthropic_api_key:
        return []

    try:
        import anthropic

        client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            messages=[{"role": "user", "content": EXTRACT_PROMPT.format(text=text)}],
        )
        return _parse_json_array(response.content[0].text)
    except Exception:
        logger.exception("Claude extraction failed")
        return []


async def extract_facts_from_result(result: dict) -> list[int]:
    """Extract factual claims from an analysis result dict via Claude. Returns list of fact_ids."""
    text = _build_text_from_result(result)
    claims = await _call_claude(text)

    layer = result.get("layer", "")
    default_topic = LAYER_TO_TOPIC.get(layer, "macro")
    default_country = result.get("country_iso3")
    source_id = result.get("id", 0)

    fact_ids = []
    for c in claims:
        if not isinstance(c, dict) or not c.get("claim"):
            continue
        fid = await store_fact(
            claim=c["claim"],
            topic=c.get("topic", default_topic),
            confidence=c.get("confidence", 0.5),
            evidence=[{
                "type": "analysis_result",
                "id": source_id,
                "summary": result.get("analysis_type", ""),
            }],
            source_type="analysis_result",
            source_id=source_id,
            subtopic=c.get("subtopic"),
            country_iso3=c.get("country_iso3") or default_country,
        )
        fact_ids.append(fid)

    return fact_ids


async def extract_facts_from_briefing(briefing: dict) -> list[int]:
    """Extract factual claims from a briefing dict via Claude. Returns list of fact_ids."""
    text = _build_text_from_briefing(briefing)
    claims = await _call_claude(text)

    default_country = briefing.get("country_iso3")
    source_id = briefing.get("id", 0)

    fact_ids = []
    for c in claims:
        if not isinstance(c, dict) or not c.get("claim"):
            continue
        fid = await store_fact(
            claim=c["claim"],
            topic=c.get("topic", "macro"),
            confidence=c.get("confidence", 0.5),
            evidence=[{
                "type": "briefing",
                "id": source_id,
                "summary": briefing.get("title", ""),
            }],
            source_type="briefing",
            source_id=source_id,
            subtopic=c.get("subtopic"),
            country_iso3=c.get("country_iso3") or default_country,
        )
        fact_ids.append(fid)

    return fact_ids


async def extract_new_facts(full: bool = False) -> int:
    """Scan analysis_results and briefings for new rows since last compile.

    Tracks progress via collection_log where source='kb_compiler'.
    Returns total number of facts extracted.
    """
    # Get last compile timestamp
    last_run = None
    if not full:
        row = await fetch_one(
            "SELECT finished_at FROM collection_log "
            "WHERE source = 'kb_compiler' AND status = 'ok' "
            "ORDER BY finished_at DESC LIMIT 1"
        )
        if row and row["finished_at"]:
            last_run = row["finished_at"]

    # Log start
    await execute(
        "INSERT INTO collection_log (source, status) VALUES ('kb_compiler', 'running')"
    )
    log_row = await fetch_one("SELECT last_insert_rowid() as id")
    log_id = log_row["id"]

    total_facts = 0

    try:
        # Fetch new analysis results
        if last_run:
            results = await fetch_all(
                "SELECT * FROM analysis_results WHERE created_at > ? ORDER BY created_at",
                (last_run,),
            )
        else:
            results = await fetch_all(
                "SELECT * FROM analysis_results ORDER BY created_at"
            )

        for r in results:
            fids = await extract_facts_from_result(dict(r))
            total_facts += len(fids)

        # Fetch new briefings
        if last_run:
            briefings = await fetch_all(
                "SELECT * FROM briefings WHERE created_at > ? ORDER BY created_at",
                (last_run,),
            )
        else:
            briefings = await fetch_all(
                "SELECT * FROM briefings ORDER BY created_at"
            )

        for b in briefings:
            fids = await extract_facts_from_briefing(dict(b))
            total_facts += len(fids)

        # Log success
        await execute(
            "UPDATE collection_log SET status = 'ok', point_count = ?, "
            "finished_at = datetime('now') WHERE id = ?",
            (total_facts, log_id),
        )

    except Exception:
        logger.exception("KB compilation failed")
        await execute(
            "UPDATE collection_log SET status = 'error', "
            "error = 'extraction failed', finished_at = datetime('now') WHERE id = ?",
            (log_id,),
        )
        raise

    return total_facts
