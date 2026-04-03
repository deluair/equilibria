"""Shared fixtures for all test modules."""

import asyncio
import os

import pytest

import app.db as db_module
from app.config import settings
from app.db import DBPool, close_db, init_db


@pytest.fixture()
async def tmp_db(tmp_path, monkeypatch):
    """Spin up a fresh in-memory-ish SQLite DB in tmp_path for each test.

    Monkeypatches the global pool and settings.db_path so no test touches the
    real equilibria.db.
    """
    db_path = str(tmp_path / "test_equilibria.db")
    monkeypatch.setattr(settings, "db_path", db_path)
    monkeypatch.setattr(settings, "db_pool_size", 2)

    # Reset the global pool state in case a previous test left it set
    monkeypatch.setattr(db_module, "_pool", None)

    await init_db()
    yield

    await close_db()
    monkeypatch.setattr(db_module, "_pool", None)


@pytest.fixture()
async def async_client(tmp_db):
    """httpx.AsyncClient wired to the FastAPI app, with DB initialised."""
    from httpx import ASGITransport, AsyncClient

    from app.main import app

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client
