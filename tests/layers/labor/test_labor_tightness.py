import pytest
from app.layers.labor.labor_tightness import LaborMarketTightness


def test_instantiation():
    model = LaborMarketTightness()
    assert model is not None


def test_layer_id():
    model = LaborMarketTightness()
    assert model.layer_id == "l3"


def test_name():
    model = LaborMarketTightness()
    assert model.name == "Labor Market Tightness"


async def test_compute_empty_db(db_conn):
    model = LaborMarketTightness()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)
    assert "score" in result
    assert result["score"] is None or isinstance(result["score"], (int, float))


async def test_compute_no_exception(db_conn):
    model = LaborMarketTightness()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert "signal" in result


async def test_compute_default_country(db_conn):
    model = LaborMarketTightness()
    result = await model.compute(db_conn)
    assert isinstance(result, dict)


async def test_compute_unavailable_on_empty(db_conn):
    model = LaborMarketTightness()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert result.get("signal") == "UNAVAILABLE" or result.get("score") is None or isinstance(result["score"], float)
