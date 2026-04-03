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


async def seed_layer_scores(db_conn, country_iso3: str = "USA", scores: dict | None = None):
    """Insert analysis_results rows for L1-L5 layers."""
    default_scores = {"l1": 20.0, "l2": 30.0, "l3": 40.0, "l4": 50.0, "l5": 60.0}
    final = scores or default_scores
    for lid, score in final.items():
        signal = "STABLE" if score < 25 else "WATCH" if score < 50 else "STRESS" if score < 75 else "CRISIS"
        await db_conn.execute(
            """
            INSERT INTO analysis_results (analysis_type, country_iso3, layer, parameters, result, score, signal)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("layer_score", country_iso3, lid, "{}", "{}", score, signal),
        )


async def seed_composite_scores(db_conn, country_iso3: str = "USA", n: int = 15, base: float = 35.0):
    """Insert n composite_score rows for signal classifier / trend tests."""
    for i in range(n):
        score = base + i * 1.0
        signal = "STABLE" if score < 25 else "WATCH" if score < 50 else "STRESS" if score < 75 else "CRISIS"
        await db_conn.execute(
            """
            INSERT INTO analysis_results (analysis_type, country_iso3, layer, parameters, result, score, signal)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("composite_score", country_iso3, "l6", "{}", "{}", score, signal),
        )
