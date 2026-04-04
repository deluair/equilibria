"""Tests for Stage 2: article compilation."""

import pytest

from app.db import execute, fetch_all, fetch_one
from app.kb.articler import get_compilable_groups


@pytest.mark.asyncio
async def test_get_compilable_groups_empty(tmp_db):
    groups = await get_compilable_groups()
    assert groups == []


@pytest.mark.asyncio
async def test_get_compilable_groups_needs_three(tmp_db):
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
