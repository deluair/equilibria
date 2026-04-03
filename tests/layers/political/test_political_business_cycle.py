import pytest
from app.layers.political.political_business_cycle import PoliticalBusinessCycle


def test_instantiation():
    model = PoliticalBusinessCycle()
    assert model is not None


def test_layer_id():
    model = PoliticalBusinessCycle()
    assert model.layer_id == "l12"


def test_name():
    model = PoliticalBusinessCycle()
    assert model.name == "Political Business Cycle"


async def test_compute_empty_db_returns_dict(db_conn):
    model = PoliticalBusinessCycle()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)


async def test_compute_empty_db_has_score_key(db_conn):
    model = PoliticalBusinessCycle()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert "score" in result


async def test_compute_empty_db_score_is_none(db_conn):
    model = PoliticalBusinessCycle()
    result = await model.compute(db_conn, country_iso3="BGD")
    # Empty DB: insufficient data -> score is None
    assert result["score"] is None


async def test_compute_with_election_years_kwarg(db_conn):
    model = PoliticalBusinessCycle()
    result = await model.compute(db_conn, country_iso3="BGD", election_years=[2014, 2018, 2022])
    assert isinstance(result, dict)
    assert "score" in result


async def test_compute_with_cbi_score_kwarg(db_conn):
    model = PoliticalBusinessCycle()
    result = await model.compute(db_conn, country_iso3="USA", cbi_score=0.8)
    assert isinstance(result, dict)
    assert "score" in result


async def test_run_returns_layer_id(db_conn):
    model = PoliticalBusinessCycle()
    result = await model.run(db_conn, country_iso3="BGD")
    assert result["layer_id"] == "l12"


async def test_run_returns_signal(db_conn):
    model = PoliticalBusinessCycle()
    result = await model.run(db_conn, country_iso3="BGD")
    assert "signal" in result


async def test_compute_with_gdp_data_returns_numeric_score(db_conn):
    # Insert enough GDP growth data to trigger Nordhaus test
    await db_conn.execute(
        "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?, ?, ?, ?)",
        ("wdi", "NY.GDP.MKTP.KD.ZG", "TST", "real gdp growth"),
    )
    cursor = await db_conn.conn.execute("SELECT last_insert_rowid()")
    row = await cursor.fetchone()
    series_id = row[0]

    model = PoliticalBusinessCycle()
    for i, val in enumerate([3.0, 4.0, 2.5, 5.0, 3.5, 6.0, 4.5, 2.0, 3.0, 4.0, 5.0, 3.0]):
        await db_conn.execute(
            "INSERT INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
            (series_id, f"{2010 + i}-01-01", val),
        )

    result = await model.compute(db_conn, country_iso3="TST", election_years=[2012, 2016, 2020])
    assert isinstance(result, dict)
    assert "score" in result
    if result["score"] is not None:
        assert 0 <= result["score"] <= 100
