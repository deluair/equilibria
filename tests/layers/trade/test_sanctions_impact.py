import pytest
from app.layers.trade.sanctions_impact import SanctionsImpact


def test_instantiation():
    model = SanctionsImpact()
    assert model is not None


def test_layer_id():
    model = SanctionsImpact()
    assert model.layer_id == "l1"


def test_name():
    model = SanctionsImpact()
    assert model.name == "Sanctions Impact"


async def test_run_empty_db_returns_dict(db_conn):
    model = SanctionsImpact()
    result = await model.run(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)


async def test_run_empty_db_has_score_key(db_conn):
    model = SanctionsImpact()
    result = await model.run(db_conn, country_iso3="BGD")
    assert "score" in result


async def test_run_empty_db_score_is_none_or_numeric(db_conn):
    model = SanctionsImpact()
    result = await model.run(db_conn, country_iso3="BGD")
    assert result["score"] is None or isinstance(result["score"], (int, float))


async def test_run_no_exception_on_empty_data(db_conn):
    model = SanctionsImpact()
    result = await model.run(db_conn, country_iso3="BGD")
    assert "score" in result


async def test_run_with_year_kwarg(db_conn):
    model = SanctionsImpact()
    result = await model.run(db_conn, country_iso3="BGD", year=2022)
    assert isinstance(result, dict)
    assert "score" in result
