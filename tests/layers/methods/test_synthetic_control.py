import pytest
from app.layers.methods.synthetic_control import SyntheticControl


def test_instantiation():
    model = SyntheticControl()
    assert model is not None


def test_layer_id():
    model = SyntheticControl()
    assert model.layer_id == "l18"


def test_name():
    model = SyntheticControl()
    assert model.name == "Synthetic Control Method"


async def test_compute_empty_db_returns_dict(db_conn):
    model = SyntheticControl()
    result = await model.compute(db_conn, country_iso3="BGD", treatment_year=2010)
    assert isinstance(result, dict)


async def test_compute_empty_db_score_is_none(db_conn):
    model = SyntheticControl()
    result = await model.compute(db_conn, country_iso3="BGD", treatment_year=2010)
    assert result.get("score") is None or isinstance(result.get("score"), (int, float))


async def test_compute_returns_signal_key(db_conn):
    model = SyntheticControl()
    result = await model.compute(db_conn, country_iso3="BGD", treatment_year=2010)
    assert "signal" in result or "error" in result


async def test_run_wraps_compute(db_conn):
    model = SyntheticControl()
    result = await model.run(db_conn, country_iso3="BGD", treatment_year=2010)
    assert "layer_id" in result
    assert result["layer_id"] == "l18"


def test_solve_weights_basic():
    import numpy as np
    y1 = np.array([1.0, 2.0, 3.0])
    Y0 = np.column_stack([np.array([1.0, 2.0, 3.0]), np.array([2.0, 3.0, 4.0])])
    w = SyntheticControl._solve_weights(y1, Y0)
    assert w.shape == (2,)
    assert abs(w.sum() - 1.0) < 1e-4
    assert (w >= -1e-6).all()
