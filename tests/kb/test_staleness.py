"""Tests for Stage 3: staleness sweep and compiler orchestrator."""

import pytest

from app.db import execute, fetch_one
from app.kb.staleness import sweep_staleness


@pytest.mark.asyncio
async def test_sweep_marks_stale_facts(tmp_db):
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
    await execute(
        "INSERT INTO kb_facts (id, claim, topic, confidence, evidence, stale_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (1, "Low confidence fact", "trade", 0.1, "[]", "2020-01-01 00:00:00"),
    )
    await sweep_staleness()
    fact = await fetch_one("SELECT confidence FROM kb_facts WHERE id = 1")
    assert fact["confidence"] == 0.1
