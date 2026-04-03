"""Tests for L16 Energy: OilMarket module."""

from __future__ import annotations

import pytest
from app.layers.energy.oil_market import OilMarket, _var_estimate, _companion_irf
import numpy as np


# ---------------------------------------------------------------------------
# Static / unit tests
# ---------------------------------------------------------------------------

def test_instantiation():
    m = OilMarket()
    assert m is not None


def test_layer_id():
    assert OilMarket.layer_id == "l16"


def test_name():
    assert OilMarket().name == "Oil Market"


def test_var_estimate_basic():
    rng = np.random.default_rng(0)
    data = rng.standard_normal((60, 3))
    res = _var_estimate(data, lags=2)
    assert "coefficients" in res
    assert "sigma" in res
    assert res["T_eff"] == 60 - 2


def test_var_estimate_raises_on_short_data():
    data = np.ones((5, 3))
    with pytest.raises(ValueError):
        _var_estimate(data, lags=4)


def test_companion_irf_shape():
    rng = np.random.default_rng(1)
    data = rng.standard_normal((50, 3))
    res = _var_estimate(data, lags=2)
    chol = np.linalg.cholesky(res["sigma"])
    irf = _companion_irf(res, chol, shock_idx=0, horizon=10)
    assert irf.shape == (11, 3)


# ---------------------------------------------------------------------------
# Async / DB-backed tests
# ---------------------------------------------------------------------------

async def test_compute_empty_db_returns_dict(db_conn):
    result = await OilMarket().compute(db_conn, country="USA")
    assert isinstance(result, dict)


async def test_compute_empty_db_has_score(db_conn):
    result = await OilMarket().compute(db_conn, country="USA")
    assert "score" in result
    assert isinstance(result["score"], float)


async def test_compute_score_in_range(db_conn):
    result = await OilMarket().compute(db_conn, country="USA")
    assert 0 <= result["score"] <= 100


async def test_compute_results_has_country(db_conn):
    result = await OilMarket().compute(db_conn, country="USA")
    assert result["results"]["country"] == "USA"
