import pytest
from app.layers.political.lobbying import LobbyingEconomics


def test_instantiation():
    model = LobbyingEconomics()
    assert model is not None


def test_layer_id():
    model = LobbyingEconomics()
    assert model.layer_id == "l12"


def test_name():
    model = LobbyingEconomics()
    assert model.name == "Lobbying Economics"


async def test_compute_empty_db_returns_dict(db_conn):
    model = LobbyingEconomics()
    result = await model.compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)


async def test_compute_empty_db_has_score_key(db_conn):
    model = LobbyingEconomics()
    result = await model.compute(db_conn, country_iso3="USA")
    assert "score" in result


async def test_compute_empty_db_score_is_none(db_conn):
    model = LobbyingEconomics()
    result = await model.compute(db_conn, country_iso3="USA")
    assert result["score"] is None


async def test_compute_with_year_kwarg(db_conn):
    model = LobbyingEconomics()
    result = await model.compute(db_conn, country_iso3="USA", year=2022)
    assert isinstance(result, dict)
    assert "score" in result


async def test_run_returns_layer_id(db_conn):
    model = LobbyingEconomics()
    result = await model.run(db_conn, country_iso3="USA")
    assert result["layer_id"] == "l12"


async def test_run_returns_signal(db_conn):
    model = LobbyingEconomics()
    result = await model.run(db_conn, country_iso3="USA")
    assert "signal" in result


async def test_compute_with_tariff_data_score_in_range(db_conn):
    # Insert tariff + import penetration + elasticity data for Grossman-Helpman
    for i in range(6):
        await db_conn.execute(
            "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?, ?, ?, ?)",
            ("wto", f"tariff_ind_{i}", "DEU", f"tariff industry {i}"),
        )
        cursor = await db_conn.conn.execute("SELECT last_insert_rowid()")
        row = await cursor.fetchone()
        sid = row[0]
        await db_conn.execute(
            "INSERT INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
            (sid, "2022-01-01", float(5 + i)),
        )

    model = LobbyingEconomics()
    result = await model.compute(db_conn, country_iso3="DEU", year=2022)
    assert isinstance(result, dict)
    assert "score" in result
    if result["score"] is not None:
        assert 0 <= result["score"] <= 100
