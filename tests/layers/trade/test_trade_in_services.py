import pytest
from app.layers.trade.trade_in_services import TradeInServices


def test_instantiation():
    model = TradeInServices()
    assert model is not None


def test_layer_id():
    model = TradeInServices()
    assert model.layer_id == "l1"


def test_name():
    model = TradeInServices()
    assert model.name == "Trade in Services"


async def test_compute_empty_db_returns_dict(db_conn):
    model = TradeInServices()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)


async def test_compute_empty_db_has_score_key(db_conn):
    model = TradeInServices()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert "score" in result


async def test_compute_empty_db_score_is_none_or_numeric(db_conn):
    model = TradeInServices()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert result["score"] is None or isinstance(result["score"], (int, float))


async def test_compute_no_exception_on_empty_data(db_conn):
    model = TradeInServices()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert "score" in result


async def test_compute_with_year_kwarg(db_conn):
    model = TradeInServices()
    result = await model.compute(db_conn, country_iso3="BGD", year=2021)
    assert isinstance(result, dict)
    assert "score" in result
