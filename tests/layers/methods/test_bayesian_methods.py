import pytest
from app.layers.methods.bayesian_methods import BayesianMethods


def test_instantiation():
    model = BayesianMethods()
    assert model is not None


def test_layer_id():
    model = BayesianMethods()
    assert model.layer_id == "l18"


def test_name():
    model = BayesianMethods()
    assert model.name == "Bayesian Methods"


async def test_compute_empty_db_returns_dict(db_conn):
    model = BayesianMethods()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)


async def test_compute_empty_db_has_score(db_conn):
    model = BayesianMethods()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert "score" in result


async def test_compute_empty_db_score_none_or_numeric(db_conn):
    model = BayesianMethods()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert result["score"] is None or isinstance(result["score"], (int, float))


async def test_run_adds_layer_meta(db_conn):
    model = BayesianMethods()
    result = await model.run(db_conn, country_iso3="BGD")
    assert result["layer_id"] == "l18"
    assert result["name"] == "Bayesian Methods"


async def test_compute_bvar_method(db_conn):
    model = BayesianMethods()
    result = await model.compute(db_conn, country_iso3="BGD", method="bvar", n_draws=100, n_burn=50)
    assert isinstance(result, dict)
    assert "score" in result
