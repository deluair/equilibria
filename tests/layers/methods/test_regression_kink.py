import pytest
from app.layers.methods.regression_kink import RegressionKinkDesign


def test_instantiation():
    model = RegressionKinkDesign()
    assert model is not None


def test_layer_id():
    model = RegressionKinkDesign()
    assert model.layer_id == "l18"


def test_name():
    model = RegressionKinkDesign()
    assert model.name == "Regression Kink Design"


async def test_compute_empty_db_returns_dict(db_conn):
    model = RegressionKinkDesign()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)


async def test_compute_empty_db_has_score(db_conn):
    model = RegressionKinkDesign()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert "score" in result


async def test_compute_empty_db_score_none_or_numeric(db_conn):
    model = RegressionKinkDesign()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert result["score"] is None or isinstance(result["score"], (int, float))


async def test_run_returns_name(db_conn):
    model = RegressionKinkDesign()
    result = await model.run(db_conn, country_iso3="BGD")
    assert result["name"] == "Regression Kink Design"


async def test_compute_with_kink_point(db_conn):
    model = RegressionKinkDesign()
    result = await model.compute(db_conn, country_iso3="BGD", kink_point=1000.0, bandwidth=500.0)
    assert isinstance(result, dict)
    assert "score" in result
