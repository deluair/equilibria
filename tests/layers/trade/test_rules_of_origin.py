import pytest
from app.layers.trade.rules_of_origin import RulesOfOrigin


def test_instantiation():
    model = RulesOfOrigin()
    assert model is not None


def test_layer_id():
    model = RulesOfOrigin()
    assert model.layer_id == "l1"


def test_name():
    model = RulesOfOrigin()
    assert model.name == "Rules of Origin"


async def test_run_empty_db_returns_dict(db_conn):
    model = RulesOfOrigin()
    result = await model.run(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)


async def test_run_empty_db_has_score_key(db_conn):
    model = RulesOfOrigin()
    result = await model.run(db_conn, country_iso3="BGD")
    assert "score" in result


async def test_run_empty_db_score_is_none_or_numeric(db_conn):
    model = RulesOfOrigin()
    result = await model.run(db_conn, country_iso3="BGD")
    assert result["score"] is None or isinstance(result["score"], (int, float))


async def test_run_no_exception_on_empty_data(db_conn):
    model = RulesOfOrigin()
    result = await model.run(db_conn, country_iso3="BGD")
    assert "score" in result


async def test_run_with_year_kwarg(db_conn):
    model = RulesOfOrigin()
    result = await model.run(db_conn, country_iso3="BGD", year=2022)
    assert isinstance(result, dict)
    assert "score" in result
