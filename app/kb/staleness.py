"""Stage 3: staleness sweep and source-driven refresh."""

import logging

from app.db import execute, fetch_all

logger = logging.getLogger(__name__)


async def sweep_staleness() -> int:
    """Mark facts as stale when stale_at has passed. Reduce confidence by 0.1 (floor 0.1).

    Returns count of facts marked stale.
    """
    rows = await fetch_all(
        "SELECT id, confidence FROM kb_facts "
        "WHERE is_stale = 0 AND stale_at < datetime('now')"
    )
    if not rows:
        return 0

    count = 0
    for row in rows:
        new_confidence = max(0.1, round(row["confidence"] - 0.1, 2))
        await execute(
            "UPDATE kb_facts SET is_stale = 1, confidence = ?, "
            "updated_at = datetime('now') WHERE id = ?",
            (new_confidence, row["id"]),
        )
        count += 1

    logger.info("Staleness sweep: marked %d facts stale", count)
    return count


async def refresh_from_sources() -> int:
    """Refresh non-stale facts whose linked source rows have newer data.

    For each non-stale fact with kb_sources entries, check if the source row
    (in analysis_results or briefings) has a created_at newer than source_date.
    If so, reset stale_at to +30 days and bump confidence by 0.05 (cap 1.0).

    Returns count of facts refreshed.
    """
    # Get non-stale facts that have source links
    facts_with_sources = await fetch_all(
        "SELECT f.id AS fact_id, f.confidence, "
        "s.source_type, s.source_id, s.source_date "
        "FROM kb_facts f "
        "JOIN kb_sources s ON s.fact_id = f.id "
        "WHERE f.is_stale = 0"
    )
    if not facts_with_sources:
        return 0

    refreshed_ids: set[int] = set()

    for row in facts_with_sources:
        fact_id = row["fact_id"]
        if fact_id in refreshed_ids:
            continue

        source_type = row["source_type"]
        source_id = row["source_id"]
        source_date = row["source_date"]

        # Determine source table
        if source_type == "analysis_result":
            table = "analysis_results"
        elif source_type == "briefing":
            table = "briefings"
        else:
            continue

        # Check if source row has newer data
        newer = await fetch_all(
            f"SELECT 1 FROM {table} WHERE id = ? AND created_at > ?",
            (source_id, source_date),
        )
        if not newer:
            continue

        new_confidence = min(1.0, round(row["confidence"] + 0.05, 2))
        await execute(
            "UPDATE kb_facts SET stale_at = datetime('now', '+30 days'), "
            "confidence = ?, updated_at = datetime('now') WHERE id = ?",
            (new_confidence, fact_id),
        )
        # Update source_date to now
        await execute(
            "UPDATE kb_sources SET source_date = datetime('now') "
            "WHERE fact_id = ? AND source_type = ? AND source_id = ?",
            (fact_id, source_type, source_id),
        )
        refreshed_ids.add(fact_id)

    logger.info("Source refresh: refreshed %d facts", len(refreshed_ids))
    return len(refreshed_ids)
