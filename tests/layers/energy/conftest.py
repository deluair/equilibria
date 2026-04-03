"""Shared fixtures for L16 Energy layer tests.

Energy modules call db.execute_fetchall(sql, params) using
    WHERE code = ?
against data_series.  The real DBConnection only exposes fetch_all; and the
real schema has series_id (text) rather than code.  We:
  1. Alias execute_fetchall -> fetch_all on the connection.
  2. Add a 'code' column to data_series and store the series code there.
"""

from __future__ import annotations

import pytest
import app.db as db_mod
from app.db import init_db, close_db, get_db, release_db


@pytest.fixture()
async def test_db(tmp_path, monkeypatch):
    db_path = str(tmp_path / "test_energy.db")
    monkeypatch.setattr(db_mod, "_pool", None)
    monkeypatch.setattr(db_mod.settings, "db_path", db_path)
    await init_db()
    yield
    await close_db()


@pytest.fixture()
async def db_conn(test_db):
    conn = await get_db()
    # Add 'code' column if absent (energy modules query WHERE code = ?)
    try:
        await conn.execute("ALTER TABLE data_series ADD COLUMN code TEXT")
    except Exception:
        pass  # column already exists
    # Alias execute_fetchall to fetch_all
    if not hasattr(conn, "execute_fetchall"):
        conn.execute_fetchall = conn.fetch_all
    yield conn
    await release_db(conn)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _insert_series(db_conn, code: str, country: str = "USA") -> int:
    """Insert a data_series row with the given code and return its id."""
    await db_conn.execute(
        "INSERT OR IGNORE INTO data_series (source, series_id, country_iso3, name, code)"
        " VALUES (?,?,?,?,?)",
        ("eia", code, country, code, code),
    )
    row = await db_conn.fetch_one(
        "SELECT id FROM data_series WHERE code=? AND country_iso3=?",
        (code, country),
    )
    return row["id"]


async def _insert_points(db_conn, series_id: int, date_value_pairs: list[tuple[str, float]]):
    await db_conn.execute_many(
        "INSERT OR IGNORE INTO data_points (series_id, date, value) VALUES (?,?,?)",
        [(series_id, d, v) for d, v in date_value_pairs],
    )
