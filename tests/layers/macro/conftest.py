import pytest
import app.db as db_mod
from app.db import init_db, close_db


@pytest.fixture()
async def test_db(tmp_path, monkeypatch):
    db_path = str(tmp_path / "test.db")
    monkeypatch.setattr(db_mod, "_db", None)
    monkeypatch.setattr(db_mod, "_pool_queue", None)
    monkeypatch.setattr(db_mod, "_pool_conns", [])
    monkeypatch.setattr(db_mod.settings, "db_path", db_path)
    await init_db()
    yield
    await close_db()
