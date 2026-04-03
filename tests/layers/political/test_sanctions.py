import pytest
from app.layers.political.sanctions import SanctionsEconomics


def test_instantiation():
    model = SanctionsEconomics()
    assert model is not None


def test_layer_id():
    model = SanctionsEconomics()
    assert model.layer_id == "l12"


def test_name():
    model = SanctionsEconomics()
    assert model.name == "Sanctions Economics"


async def test_compute_empty_db_returns_dict(db_conn):
    model = SanctionsEconomics()
    result = await model.compute(db_conn, country_iso3="IRN")
    assert isinstance(result, dict)


async def test_compute_empty_db_has_score_key(db_conn):
    model = SanctionsEconomics()
    result = await model.compute(db_conn, country_iso3="IRN")
    assert "score" in result


async def test_compute_empty_db_score_is_none(db_conn):
    model = SanctionsEconomics()
    result = await model.compute(db_conn, country_iso3="IRN")
    assert result["score"] is None


async def test_compute_with_sender_kwarg(db_conn):
    model = SanctionsEconomics()
    result = await model.compute(db_conn, country_iso3="IRN", sender="USA")
    assert isinstance(result, dict)
    assert "score" in result


async def test_run_returns_layer_id(db_conn):
    model = SanctionsEconomics()
    result = await model.run(db_conn, country_iso3="IRN")
    assert result["layer_id"] == "l12"


async def test_run_returns_signal(db_conn):
    model = SanctionsEconomics()
    result = await model.run(db_conn, country_iso3="IRN")
    assert "signal" in result


async def test_compute_with_sanctions_data_score_in_range(db_conn):
    # Insert a sanctions series
    await db_conn.execute(
        "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?, ?, ?, ?)",
        ("hseo", "SANCTION_ACTIVE", "RUS", "sanction active"),
    )
    cursor = await db_conn.conn.execute("SELECT last_insert_rowid()")
    row = await cursor.fetchone()
    sid = row[0]
    for yr in range(2014, 2024):
        await db_conn.execute(
            "INSERT INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
            (sid, f"{yr}-01-01", 1.0),
        )

    model = SanctionsEconomics()
    result = await model.compute(db_conn, country_iso3="RUS")
    assert isinstance(result, dict)
    assert "score" in result
    if result["score"] is not None:
        assert 0 <= result["score"] <= 100


async def test_compute_returns_sanctions_active_field(db_conn):
    # Insert GDP growth data only (no sanctions) -> sanctions_active = False
    await db_conn.execute(
        "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?, ?, ?, ?)",
        ("wdi", "NY.GDP.MKTP.KD.ZG", "KOR", "real gdp growth"),
    )
    cursor = await db_conn.conn.execute("SELECT last_insert_rowid()")
    row = await cursor.fetchone()
    sid = row[0]
    for i in range(10):
        await db_conn.execute(
            "INSERT INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
            (sid, f"{2010 + i}-01-01", 3.0 + i * 0.1),
        )

    model = SanctionsEconomics()
    result = await model.compute(db_conn, country_iso3="KOR")
    assert "sanctions_active" in result
    assert result["sanctions_active"] is False
