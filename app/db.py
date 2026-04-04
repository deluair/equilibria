import asyncio
import logging
from pathlib import Path

import aiosqlite

from app.config import settings

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS countries (
    iso3 TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    region TEXT,
    income_group TEXT,
    population INTEGER,
    gdp_usd REAL,
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS data_series (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    series_id TEXT NOT NULL,
    country_iso3 TEXT,
    name TEXT NOT NULL,
    description TEXT,
    unit TEXT,
    frequency TEXT,
    metadata TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(source, series_id, country_iso3)
);

CREATE TABLE IF NOT EXISTS data_points (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    series_id INTEGER NOT NULL REFERENCES data_series(id),
    date TEXT NOT NULL,
    value REAL NOT NULL,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(series_id, date)
);

CREATE TABLE IF NOT EXISTS analysis_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    analysis_type TEXT NOT NULL,
    country_iso3 TEXT,
    layer TEXT,
    parameters TEXT,
    result TEXT NOT NULL,
    score REAL,
    signal TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS briefings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    country_iso3 TEXT NOT NULL,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    layer_scores TEXT,
    composite_score REAL,
    signal TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS collection_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    series_count INTEGER DEFAULT 0,
    point_count INTEGER DEFAULT 0,
    status TEXT NOT NULL,
    error TEXT,
    started_at TEXT DEFAULT (datetime('now')),
    finished_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_data_points_series ON data_points(series_id);
CREATE INDEX IF NOT EXISTS idx_data_points_date ON data_points(date);
CREATE INDEX IF NOT EXISTS idx_data_series_source ON data_series(source);
CREATE INDEX IF NOT EXISTS idx_data_series_country ON data_series(country_iso3);
CREATE INDEX IF NOT EXISTS idx_analysis_results_type ON analysis_results(analysis_type);
CREATE INDEX IF NOT EXISTS idx_analysis_results_country ON analysis_results(country_iso3);
CREATE INDEX IF NOT EXISTS idx_briefings_country ON briefings(country_iso3);
CREATE INDEX IF NOT EXISTS idx_collection_log_source ON collection_log(source);

CREATE TABLE IF NOT EXISTS conversations (
    id TEXT PRIMARY KEY,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS conversation_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id TEXT NOT NULL REFERENCES conversations(id),
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_conv_messages_conv ON conversation_messages(conversation_id);
"""


class DBConnection:
    """Wrapper around aiosqlite connection."""

    def __init__(self, conn: aiosqlite.Connection):
        self.conn = conn

    async def fetch_one(self, sql: str, params: tuple = ()) -> dict | None:
        cursor = await self.conn.execute(sql, params)
        row = await cursor.fetchone()
        if row is None:
            return None
        columns = [d[0] for d in cursor.description]
        return dict(zip(columns, row))

    async def fetch_all(self, sql: str, params: tuple = ()) -> list[dict]:
        cursor = await self.conn.execute(sql, params)
        rows = await cursor.fetchall()
        columns = [d[0] for d in cursor.description]
        return [dict(zip(columns, row)) for row in rows]

    async def execute(self, sql: str, params: tuple = ()) -> int:
        cursor = await self.conn.execute(sql, params)
        await self.conn.commit()
        return cursor.rowcount

    async def execute_many(self, sql: str, params_list: list[tuple]) -> int:
        await self.conn.executemany(sql, params_list)
        await self.conn.commit()
        return len(params_list)


class DBPool:
    """Simple async connection pool using asyncio.Queue."""

    def __init__(self, db_path: str, pool_size: int = 5):
        self.db_path = db_path
        self.pool_size = pool_size
        self._pool: asyncio.Queue[aiosqlite.Connection] = asyncio.Queue(maxsize=pool_size)
        self._initialized = False

    async def init(self):
        if self._initialized:
            return
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        for _ in range(self.pool_size):
            conn = await aiosqlite.connect(self.db_path)
            await conn.execute("PRAGMA journal_mode=WAL")
            await conn.execute("PRAGMA foreign_keys=ON")
            await conn.execute("PRAGMA busy_timeout=5000")
            conn.row_factory = aiosqlite.Row
            self._pool.put_nowait(conn)
        self._initialized = True
        logger.info("DB pool initialized with %d connections", self.pool_size)

    async def acquire(self) -> DBConnection:
        conn = await self._pool.get()
        return DBConnection(conn)

    async def release(self, db: DBConnection):
        await self._pool.put(db.conn)

    async def close(self):
        while not self._pool.empty():
            conn = self._pool.get_nowait()
            await conn.close()
        self._initialized = False
        logger.info("DB pool closed")


_pool: DBPool | None = None
_db: None = None
_pool_queue: None = None
_pool_conns: list = []


async def get_db() -> DBConnection:
    """Get a connection from the global pool."""
    if _pool is None:
        raise RuntimeError("DB pool not initialized. Call init_db() first.")
    return await _pool.acquire()


async def release_db(db: DBConnection):
    """Return a connection to the pool."""
    if _pool is not None:
        await _pool.release(db)


async def init_db():
    """Initialize the connection pool and create schema."""
    global _pool
    _pool = DBPool(settings.db_path, settings.db_pool_size)
    await _pool.init()
    db = await get_db()
    try:
        await db.conn.executescript(SCHEMA)
        await db.conn.commit()
        logger.info("Database schema initialized")
    finally:
        await release_db(db)


async def close_db():
    """Close all connections in the pool."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


# Convenience functions that acquire/release automatically
async def fetch_one(sql: str, params: tuple = ()) -> dict | None:
    db = await get_db()
    try:
        return await db.fetch_one(sql, params)
    finally:
        await release_db(db)


async def fetch_all(sql: str, params: tuple = ()) -> list[dict]:
    db = await get_db()
    try:
        return await db.fetch_all(sql, params)
    finally:
        await release_db(db)


async def execute(sql: str, params: tuple = ()) -> int:
    db = await get_db()
    try:
        return await db.execute(sql, params)
    finally:
        await release_db(db)


async def execute_many(sql: str, params_list: list[tuple]) -> int:
    db = await get_db()
    try:
        return await db.execute_many(sql, params_list)
    finally:
        await release_db(db)
