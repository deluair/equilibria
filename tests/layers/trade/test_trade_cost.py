import pytest
from app.layers.trade.trade_cost import TradeCost


def test_instantiation():
    model = TradeCost()
    assert model is not None


def test_layer_id():
    model = TradeCost()
    assert model.layer_id == "l1"


def test_name():
    model = TradeCost()
    assert model.name == "Trade Cost (Novy)"


async def test_run_empty_db_returns_dict(db_conn):
    model = TradeCost()
    result = await model.run(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)


async def test_run_empty_db_has_score_key(db_conn):
    model = TradeCost()
    result = await model.run(db_conn, country_iso3="BGD")
    assert "score" in result


async def test_run_empty_db_score_is_none_or_numeric(db_conn):
    model = TradeCost()
    result = await model.run(db_conn, country_iso3="BGD")
    assert result["score"] is None or isinstance(result["score"], (int, float))


async def test_run_no_exception_on_empty_data(db_conn):
    model = TradeCost()
    result = await model.run(db_conn, country_iso3="BGD")
    assert "score" in result


async def test_run_with_partner_kwarg(db_conn):
    model = TradeCost()
    result = await model.run(db_conn, country_iso3="BGD", partner_iso3="USA")
    assert isinstance(result, dict)
    assert "score" in result
