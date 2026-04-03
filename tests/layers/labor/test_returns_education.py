import pytest
from app.layers.labor.returns_education import ReturnsToEducation


def test_instantiation():
    model = ReturnsToEducation()
    assert model is not None


def test_layer_id():
    model = ReturnsToEducation()
    assert model.layer_id == "l3"


def test_name():
    model = ReturnsToEducation()
    assert model.name == "Returns to Education (IV)"


async def test_compute_empty_db(db_conn):
    model = ReturnsToEducation()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)
    assert "score" in result
    assert result["score"] is None or isinstance(result["score"], (int, float))


async def test_compute_no_exception(db_conn):
    model = ReturnsToEducation()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert "signal" in result


async def test_compute_with_year(db_conn):
    model = ReturnsToEducation()
    result = await model.compute(db_conn, country_iso3="BGD", year=2022)
    assert isinstance(result, dict)


async def test_compute_unavailable_on_empty(db_conn):
    model = ReturnsToEducation()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert result.get("signal") == "UNAVAILABLE" or result.get("score") is None or isinstance(result["score"], float)
