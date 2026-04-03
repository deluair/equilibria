import pytest
from app.layers.health.disease_burden import DiseaseBurden


def test_instantiation():
    m = DiseaseBurden()
    assert m is not None


def test_layer_id():
    m = DiseaseBurden()
    assert m.layer_id == "l8"


def test_name():
    m = DiseaseBurden()
    assert m.name == "Disease Burden"


async def test_compute_empty_db_returns_dict(db_conn):
    m = DiseaseBurden()
    result = await m.compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)


async def test_compute_empty_db_fallback_score(db_conn):
    m = DiseaseBurden()
    result = await m.compute(db_conn, country_iso3="BGD")
    assert "score" in result
    assert result["score"] == 50


async def test_compute_no_country_iso3(db_conn):
    m = DiseaseBurden()
    result = await m.compute(db_conn)
    assert isinstance(result, dict)
    assert "score" in result


async def test_compute_results_key_present(db_conn):
    m = DiseaseBurden()
    result = await m.compute(db_conn, country_iso3="BGD")
    assert "results" in result
    # empty DB hits early-return error path
    assert "error" in result["results"]


async def test_compute_with_data_has_full_keys(db_conn):
    # Seed minimal life expectancy + GDP rows so compute() passes the guard
    await db_conn.execute(
        "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?, ?, ?, ?)",
        ("WDI", "SP.DYN.LE00.IN", "BGD", "Life expectancy"),
    )
    le_sid = (await db_conn.fetch_one("SELECT id FROM data_series WHERE series_id='SP.DYN.LE00.IN'"))["id"]
    await db_conn.execute(
        "INSERT INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
        (le_sid, "2020-01-01", 72.5),
    )
    await db_conn.execute(
        "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?, ?, ?, ?)",
        ("WDI", "NY.GDP.PCAP.KD", "BGD", "GDP per capita"),
    )
    gdp_sid = (await db_conn.fetch_one("SELECT id FROM data_series WHERE series_id='NY.GDP.PCAP.KD'"))["id"]
    await db_conn.execute(
        "INSERT INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
        (gdp_sid, "2020-01-01", 1800.0),
    )
    m = DiseaseBurden()
    result = await m.compute(db_conn, country_iso3="BGD")
    assert "results" in result
    r = result["results"]
    assert "daly" in r
    assert "epidemiological_transition" in r
    assert "mortality_decomposition" in r
    assert "preston_curve" in r
