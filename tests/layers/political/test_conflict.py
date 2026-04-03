import pytest
from app.layers.political.conflict import ConflictEconomics


def test_instantiation():
    model = ConflictEconomics()
    assert model is not None


def test_layer_id():
    model = ConflictEconomics()
    assert model.layer_id == "l12"


def test_name():
    model = ConflictEconomics()
    assert model.name == "Conflict Economics"


async def test_compute_empty_db_returns_dict(db_conn):
    model = ConflictEconomics()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)


async def test_compute_empty_db_has_score_key(db_conn):
    model = ConflictEconomics()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert "score" in result


async def test_compute_empty_db_score_is_none(db_conn):
    model = ConflictEconomics()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert result["score"] is None


async def test_run_returns_layer_id(db_conn):
    model = ConflictEconomics()
    result = await model.run(db_conn, country_iso3="BGD")
    assert result["layer_id"] == "l12"


async def test_run_returns_signal(db_conn):
    model = ConflictEconomics()
    result = await model.run(db_conn, country_iso3="BGD")
    assert "signal" in result


async def test_compute_with_gdp_per_capita_data(db_conn):
    # Insert GDP per capita (low income = high conflict risk)
    await db_conn.execute(
        "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?, ?, ?, ?)",
        ("wdi", "NY.GDP.PCAP.CD", "SSD", "gdp per capita"),
    )
    cursor = await db_conn.conn.execute("SELECT last_insert_rowid()")
    row = await cursor.fetchone()
    sid = row[0]
    for i in range(5):
        await db_conn.execute(
            "INSERT INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
            (sid, f"{2018 + i}-01-01", 1200.0 + i * 50),
        )

    model = ConflictEconomics()
    result = await model.compute(db_conn, country_iso3="SSD")
    assert isinstance(result, dict)
    assert "score" in result
    if result["score"] is not None:
        assert 0 <= result["score"] <= 100


async def test_compute_with_political_stability_data(db_conn):
    # Insert WGI political stability series
    await db_conn.execute(
        "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?, ?, ?, ?)",
        ("wgi", "PV.EST", "IRQ", "political stability"),
    )
    cursor = await db_conn.conn.execute("SELECT last_insert_rowid()")
    row = await cursor.fetchone()
    sid = row[0]
    await db_conn.execute(
        "INSERT INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
        (sid, "2022-01-01", -2.0),
    )

    model = ConflictEconomics()
    result = await model.compute(db_conn, country_iso3="IRQ")
    assert isinstance(result, dict)
    assert "score" in result
    if result["score"] is not None:
        assert 0 <= result["score"] <= 100


async def test_compute_score_in_valid_range_with_risk_data(db_conn):
    # Insert resource rents data
    await db_conn.execute(
        "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?, ?, ?, ?)",
        ("wdi", "NY.GDP.TOTL.RT.ZS", "COD", "natural resource rents gdp"),
    )
    cursor = await db_conn.conn.execute("SELECT last_insert_rowid()")
    row = await cursor.fetchone()
    sid = row[0]
    await db_conn.execute(
        "INSERT INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
        (sid, "2022-01-01", 25.0),
    )

    model = ConflictEconomics()
    result = await model.compute(db_conn, country_iso3="COD")
    assert isinstance(result, dict)
    assert "score" in result
    if result["score"] is not None:
        assert 0 <= result["score"] <= 100
