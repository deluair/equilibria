import pytest
from app.layers.health.health_insurance import HealthInsurance


def test_instantiation():
    m = HealthInsurance()
    assert m is not None


def test_layer_id():
    m = HealthInsurance()
    assert m.layer_id == "l8"


def test_name():
    m = HealthInsurance()
    assert m.name == "Health Insurance"


async def test_compute_empty_db_returns_dict(db_conn):
    m = HealthInsurance()
    result = await m.compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)


async def test_compute_empty_db_fallback_score(db_conn):
    m = HealthInsurance()
    result = await m.compute(db_conn, country_iso3="BGD")
    assert "score" in result
    assert result["score"] == 50


async def test_compute_no_country_iso3(db_conn):
    m = HealthInsurance()
    result = await m.compute(db_conn)
    assert isinstance(result, dict)
    assert "score" in result


async def test_compute_results_error_on_empty_db(db_conn):
    m = HealthInsurance()
    result = await m.compute(db_conn, country_iso3="BGD")
    assert "results" in result
    assert "error" in result["results"]


async def test_compute_results_keys_present_with_data(db_conn):
    # Seed minimal UHC row so compute() passes the guard and returns full result keys
    await db_conn.execute(
        "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?, ?, ?, ?)",
        ("WDI", "SH.UHC.SRVS.CV.XD", "BGD", "UHC index"),
    )
    sid = (await db_conn.fetch_one("SELECT id FROM data_series WHERE series_id='SH.UHC.SRVS.CV.XD'"))["id"]
    await db_conn.execute(
        "INSERT INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
        (sid, "2019-01-01", 55.0),
    )
    m = HealthInsurance()
    result = await m.compute(db_conn, country_iso3="BGD")
    assert "results" in result
    r = result["results"]
    assert "adverse_selection" in r
    assert "moral_hazard" in r
    assert "uhc_coverage" in r
    assert "country_iso3" in r
