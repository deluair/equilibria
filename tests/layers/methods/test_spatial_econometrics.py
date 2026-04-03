import pytest
from app.layers.methods.spatial_econometrics import SpatialEconometrics


def test_instantiation():
    model = SpatialEconometrics()
    assert model is not None


def test_layer_id():
    model = SpatialEconometrics()
    assert model.layer_id == "l18"


def test_name():
    model = SpatialEconometrics()
    assert model.name == "Spatial Econometrics"


async def test_compute_empty_db_returns_dict(db_conn):
    model = SpatialEconometrics()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)


async def test_compute_empty_db_has_score(db_conn):
    model = SpatialEconometrics()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert "score" in result


async def test_compute_empty_db_score_none_or_numeric(db_conn):
    model = SpatialEconometrics()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert result["score"] is None or isinstance(result["score"], (int, float))


async def test_run_adds_layer_id(db_conn):
    model = SpatialEconometrics()
    result = await model.run(db_conn, country_iso3="BGD")
    assert "layer_id" in result
    assert result["layer_id"] == "l18"


async def test_compute_weights_type_distance(db_conn):
    model = SpatialEconometrics()
    result = await model.compute(db_conn, country_iso3="BGD", weights_type="distance")
    assert isinstance(result, dict)
    assert "score" in result
