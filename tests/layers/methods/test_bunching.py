import pytest
from app.layers.methods.bunching import BunchingEstimation


def test_instantiation():
    model = BunchingEstimation()
    assert model is not None


def test_layer_id():
    model = BunchingEstimation()
    assert model.layer_id == "l18"


def test_name():
    model = BunchingEstimation()
    assert model.name == "Bunching Estimation"


async def test_compute_empty_db_returns_dict(db_conn):
    model = BunchingEstimation()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)


async def test_compute_empty_db_has_score(db_conn):
    model = BunchingEstimation()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert "score" in result


async def test_compute_empty_db_score_none_or_numeric(db_conn):
    model = BunchingEstimation()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert result["score"] is None or isinstance(result["score"], (int, float))


async def test_run_returns_layer_id(db_conn):
    model = BunchingEstimation()
    result = await model.run(db_conn, country_iso3="BGD")
    assert result["layer_id"] == "l18"


async def test_compute_with_threshold(db_conn):
    model = BunchingEstimation()
    result = await model.compute(db_conn, country_iso3="BGD", threshold=50000.0, bunching_type="kink")
    assert isinstance(result, dict)
    assert "score" in result
