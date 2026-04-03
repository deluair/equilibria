"""Shared fixtures for L15 Monetary layer tests.

Monetary modules call db.execute_fetchall() directly (aiosqlite native method),
so tests must pass db_conn.conn (the raw aiosqlite.Connection) to compute().
"""

from __future__ import annotations

import pytest
import app.db as db_mod
from app.db import init_db, close_db, get_db, release_db


@pytest.fixture()
async def test_db(tmp_path, monkeypatch):
    db_path = str(tmp_path / "test_monetary.db")
    monkeypatch.setattr(db_mod, "_pool", None)
    monkeypatch.setattr(db_mod.settings, "db_path", db_path)
    await init_db()
    yield
    await close_db()


@pytest.fixture()
async def db_conn(test_db):
    """Yield the DBConnection wrapper (for schema ops)."""
    conn = await get_db()
    yield conn
    await release_db(conn)


@pytest.fixture()
async def raw_conn(db_conn):
    """Yield the raw aiosqlite.Connection needed by monetary compute() methods."""
    yield db_conn.conn


async def _insert_series(db_conn, code: str, name: str | None = None) -> int:
    """Insert a data_series by code (series_id) and return its id."""
    label = name or code
    await db_conn.execute(
        "INSERT OR IGNORE INTO data_series (source, series_id, country_iso3, name) VALUES (?, ?, ?, ?)",
        ("test", code, "USA", label),
    )
    row = await db_conn.fetch_one(
        "SELECT id FROM data_series WHERE series_id=? AND source='test'",
        (code,),
    )
    return row["id"]


async def _insert_points(db_conn, series_db_id: int, date_value_pairs: list[tuple[str, float]]):
    """Bulk-insert (date, value) rows for a given series db id."""
    await db_conn.execute_many(
        "INSERT OR IGNORE INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
        [(series_db_id, d, v) for d, v in date_value_pairs],
    )
