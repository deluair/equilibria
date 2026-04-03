import pytest
from app.layers.methods.mixture_models import MixtureModels


def test_instantiation():
    model = MixtureModels()
    assert model is not None


def test_layer_id():
    model = MixtureModels()
    assert model.layer_id == "l18"


def test_name():
    model = MixtureModels()
    assert model.name == "Mixture Models"


async def test_compute_empty_db_returns_dict(db_conn):
    model = MixtureModels()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)


async def test_compute_empty_db_has_score(db_conn):
    model = MixtureModels()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert "score" in result


async def test_compute_empty_db_score_none_or_numeric(db_conn):
    model = MixtureModels()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert result["score"] is None or isinstance(result["score"], (int, float))


async def test_run_adds_layer_id(db_conn):
    model = MixtureModels()
    result = await model.run(db_conn, country_iso3="BGD")
    assert result["layer_id"] == "l18"


async def test_compute_max_components_param(db_conn):
    model = MixtureModels()
    result = await model.compute(db_conn, country_iso3="BGD", max_components=3)
    assert isinstance(result, dict)
    assert "score" in result
