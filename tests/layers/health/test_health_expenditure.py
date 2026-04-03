import pytest
from app.layers.health.health_expenditure import HealthExpenditure, _dea_output_oriented
import numpy as np


def test_instantiation():
    m = HealthExpenditure()
    assert m is not None


def test_layer_id():
    m = HealthExpenditure()
    assert m.layer_id == "l8"


def test_name():
    m = HealthExpenditure()
    assert m.name == "Health Expenditure"


async def test_compute_empty_db_returns_dict(db_conn):
    m = HealthExpenditure()
    result = await m.compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)


async def test_compute_empty_db_has_score(db_conn):
    m = HealthExpenditure()
    result = await m.compute(db_conn, country_iso3="BGD")
    assert "score" in result
    assert result["score"] == 50


async def test_compute_no_country_iso3(db_conn):
    m = HealthExpenditure()
    result = await m.compute(db_conn)
    assert isinstance(result, dict)
    assert "score" in result


async def test_compute_results_key_present(db_conn):
    m = HealthExpenditure()
    result = await m.compute(db_conn, country_iso3="BGD")
    assert "results" in result


def test_dea_output_oriented_single_dmu():
    inputs = np.array([[100.0]])
    outputs = np.array([[75.0]])
    scores = _dea_output_oriented(inputs, outputs)
    assert scores.shape == (1,)
    assert scores[0] == pytest.approx(1.0, abs=1e-6)


def test_dea_output_oriented_frontier_unit():
    # One unit dominates all others in output per input -> score 1.0
    inputs = np.array([[10.0], [20.0], [30.0]])
    outputs = np.array([[80.0], [70.0], [60.0]])
    scores = _dea_output_oriented(inputs, outputs)
    assert scores[0] == pytest.approx(1.0, abs=1e-4)
    assert all(0.0 < s <= 1.0 for s in scores)
