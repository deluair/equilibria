"""Shared fixtures for L17 Demographic layer tests.

Demographic modules call db.fetch_all(sql) with WDI series_id codes.
The schema stores them in data_series.series_id (text).
"""

from __future__ import annotations

import pytest
import app.db as db_mod
from app.db import init_db, close_db, get_db, release_db


@pytest.fixture()
async def test_db(tmp_path, monkeypatch):
    db_path = str(tmp_path / "test_demographic.db")
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _insert_wdi_series(db_conn, series_id: str, country_iso3: str) -> int:
    """Insert a data_series row for a WDI indicator and return its id."""
    await db_conn.execute(
        "INSERT OR IGNORE INTO data_series (source, series_id, country_iso3, name)"
        " VALUES (?,?,?,?)",
        ("wdi", series_id, country_iso3, series_id),
    )
    row = await db_conn.fetch_one(
        "SELECT id FROM data_series WHERE series_id=? AND country_iso3=?",
        (series_id, country_iso3),
    )
    return row["id"]


async def _insert_points(db_conn, series_db_id: int, year_value_pairs: list[tuple[str, float]]):
    """Insert (date, value) rows for a series."""
    await db_conn.execute_many(
        "INSERT OR IGNORE INTO data_points (series_id, date, value) VALUES (?,?,?)",
        [(series_db_id, yr, val) for yr, val in year_value_pairs],
    )
