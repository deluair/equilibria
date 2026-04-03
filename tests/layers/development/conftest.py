import pytest
import app.db as db_mod
from app.db import init_db, close_db, get_db, release_db


@pytest.fixture()
async def test_db(tmp_path, monkeypatch):
    db_path = str(tmp_path / "test.db")
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


async def insert_series(db_conn, series_id: str, country_iso3: str) -> int:
    """Insert a data_series row and return its id."""
    cursor = await db_conn.conn.execute(
        "INSERT OR IGNORE INTO data_series (source, series_id, country_iso3, name) VALUES (?, ?, ?, ?)",
        ("test", series_id, country_iso3, series_id),
    )
    await db_conn.conn.commit()
    row = await db_conn.conn.execute(
        "SELECT id FROM data_series WHERE series_id = ? AND country_iso3 = ?",
        (series_id, country_iso3),
    )
    r = await row.fetchone()
    return r[0]


async def insert_point(db_conn, series_row_id: int, date: str, value: float):
    """Insert a data_points row."""
    await db_conn.conn.execute(
        "INSERT OR IGNORE INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
        (series_row_id, date, value),
    )
    await db_conn.conn.commit()


async def insert_country(db_conn, iso3: str, region: str = "TestRegion", income_group: str = "Upper middle income"):
    """Insert a country row."""
    await db_conn.conn.execute(
        "INSERT OR IGNORE INTO countries (iso3, name, region, income_group) VALUES (?, ?, ?, ?)",
        (iso3, iso3, region, income_group),
    )
    await db_conn.conn.commit()
