import pytest
from app.layers.methods.quantile_regression import QuantileRegression


def test_instantiation():
    model = QuantileRegression()
    assert model is not None


def test_layer_id():
    model = QuantileRegression()
    assert model.layer_id == "l18"


def test_name():
    model = QuantileRegression()
    assert model.name == "Quantile Regression"


async def test_compute_empty_db_returns_dict(db_conn):
    model = QuantileRegression()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)


async def test_compute_empty_db_has_score(db_conn):
    model = QuantileRegression()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert "score" in result


async def test_compute_empty_db_score_none_or_numeric(db_conn):
    model = QuantileRegression()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert result["score"] is None or isinstance(result["score"], (int, float))


async def test_run_adds_name(db_conn):
    model = QuantileRegression()
    result = await model.run(db_conn, country_iso3="BGD")
    assert result["name"] == "Quantile Regression"


async def test_compute_custom_quantiles(db_conn):
    model = QuantileRegression()
    result = await model.compute(db_conn, country_iso3="BGD", quantiles=[0.25, 0.50, 0.75])
    assert isinstance(result, dict)
    assert "score" in result
