"""KB compiler orchestrator: runs all 3 stages in sequence."""

import logging
from datetime import datetime, timezone

from app.db import execute
from app.kb.extractor import extract_new_facts
from app.kb.staleness import refresh_from_sources, sweep_staleness

logger = logging.getLogger(__name__)


async def compile_kb(full: bool = False) -> dict:
    """Run the full KB compilation pipeline.

    Stages:
        1. Extract new facts from analysis_results and briefings
        2. Compile articles from clustered facts
        3. Refresh sources, then sweep staleness

    Args:
        full: If True, re-extract from all rows (not just since last compile).

    Returns:
        Dict with facts_extracted, articles_compiled, facts_refreshed,
        facts_swept, started_at, finished_at.
    """
    started_at = datetime.now(timezone.utc).isoformat()

    # Stage 1: extract facts
    facts_extracted = await extract_new_facts(full)

    # Stage 2: compile articles
    try:
        from app.kb.articler import compile_articles

        articles_compiled = await compile_articles()
    except ImportError:
        logger.warning("articler not yet available, skipping Stage 2")
        articles_compiled = 0

    # Stage 3: refresh then sweep
    facts_refreshed = await refresh_from_sources()
    facts_swept = await sweep_staleness()

    finished_at = datetime.now(timezone.utc).isoformat()

    # Log compile run
    await execute(
        "INSERT INTO collection_log (source, series_count, point_count, status, "
        "started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
        (
            "kb_compiler",
            articles_compiled,
            facts_extracted,
            "ok",
            started_at,
            finished_at,
        ),
    )

    result = {
        "facts_extracted": facts_extracted,
        "articles_compiled": articles_compiled,
        "facts_refreshed": facts_refreshed,
        "facts_swept": facts_swept,
        "started_at": started_at,
        "finished_at": finished_at,
    }
    logger.info("KB compile complete: %s", result)
    return result
