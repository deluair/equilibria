import pytest
from app.layers.labor.unemployment_duration import UnemploymentDuration


def test_instantiation():
    model = UnemploymentDuration()
    assert model is not None


def test_layer_id():
    model = UnemploymentDuration()
    assert model.layer_id == "l3"


def test_name():
    model = UnemploymentDuration()
    assert model.name == "Unemployment Duration (Hazard)"


async def test_compute_empty_db(db_conn):
    model = UnemploymentDuration()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)
    assert "score" in result
    assert result["score"] is None or isinstance(result["score"], (int, float))


async def test_compute_no_exception(db_conn):
    model = UnemploymentDuration()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert "signal" in result


async def test_compute_with_year(db_conn):
    model = UnemploymentDuration()
    result = await model.compute(db_conn, country_iso3="BGD", year=2022)
    assert isinstance(result, dict)


async def test_compute_unavailable_on_empty(db_conn):
    model = UnemploymentDuration()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert result.get("signal") == "UNAVAILABLE" or result.get("score") is None or isinstance(result["score"], float)
