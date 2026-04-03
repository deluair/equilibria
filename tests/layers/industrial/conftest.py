"""Shared fixtures for L14 Industrial Organization layer tests."""

from __future__ import annotations

import json

import pytest
import app.db as db_mod
from app.db import init_db, close_db, get_db, release_db


@pytest.fixture()
async def test_db(tmp_path, monkeypatch):
    db_path = str(tmp_path / "test_industrial.db")
    monkeypatch.setattr(db_mod, "_pool", None)
    monkeypatch.setattr(db_mod.settings, "db_path", db_path)
    await init_db()
    yield
    await close_db()


@pytest.fixture()
async def db_conn(test_db):
    conn = await get_db()
    yield conn
    await release_db(conn)


async def _insert_series(db_conn, source: str, series_id: str, country: str,
                          description: str, metadata: dict | None = None) -> int:
    """Insert a data_series row and return its id."""
    meta_str = json.dumps(metadata) if metadata else None
    await db_conn.execute(
        """
        INSERT OR IGNORE INTO data_series (source, series_id, country_iso3, name, description, metadata)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (source, series_id, country, description, description, meta_str),
    )
    row = await db_conn.fetch_one(
        "SELECT id FROM data_series WHERE source=? AND series_id=? AND country_iso3=?",
        (source, series_id, country),
    )
    return row["id"]


async def _insert_points(db_conn, series_id: int, date_value_pairs: list[tuple[str, float]]):
    """Bulk-insert (date, value) rows for a given series_id."""
    await db_conn.execute_many(
        "INSERT OR IGNORE INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
        [(series_id, d, v) for d, v in date_value_pairs],
    )
