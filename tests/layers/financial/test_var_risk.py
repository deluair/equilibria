"""Tests for L7 Financial: Value at Risk module."""

from __future__ import annotations

import numpy as np
import pytest
from app.layers.financial.var_risk import ValueAtRisk
from tests.layers.financial.conftest import _insert_series, _insert_points


def test_instantiation():
    assert ValueAtRisk() is not None


def test_layer_id():
    assert ValueAtRisk().layer_id == "l7"


def test_name():
    assert ValueAtRisk().name == "Value at Risk"


async def test_compute_empty_db_returns_unavailable(db_conn):
    result = await ValueAtRisk().compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)
    assert result["score"] is None
    assert result["signal"] == "UNAVAILABLE"


async def test_compute_with_sufficient_data_returns_score(db_conn):
    """Insert 100 daily return rows (>= 60 required)."""
    rng = np.random.default_rng(10)
    returns = rng.normal(0.0, 0.015, 100)
    dates = [f"2022-{(i // 30 + 1):02d}-{(i % 28 + 1):02d}" for i in range(100)]
    sid = await _insert_series(db_conn, "yahoo", "mkt_idx", "USA",
                                "market_index asset_returns daily")
    await _insert_points(db_conn, sid, list(zip(dates, returns.tolist())))

    result = await ValueAtRisk().compute(db_conn, country_iso3="USA",
                                          portfolio_id="mkt_idx")
    assert isinstance(result.get("score"), float)
    assert 0.0 <= result["score"] <= 100.0


async def test_historical_var_is_negative_for_losses():
    rng = np.random.default_rng(11)
    returns = rng.normal(0.0, 0.02, 500)
    var = ValueAtRisk._historical_var(returns, alpha=0.01, horizon=1)
    # At 1% alpha, VaR should be in the left tail (negative or small)
    assert var < np.mean(returns)


async def test_historical_es_lte_historical_var():
    rng = np.random.default_rng(12)
    returns = rng.normal(0.0, 0.02, 500)
    var = ValueAtRisk._historical_var(returns, alpha=0.01, horizon=1)
    es = ValueAtRisk._historical_es(returns, alpha=0.01, horizon=1)
    assert es <= var + 1e-9


async def test_parametric_var_normal_shape():
    rng = np.random.default_rng(13)
    returns = rng.normal(0.001, 0.02, 300)
    var, es = ValueAtRisk._parametric_var_normal(returns, alpha=0.05, horizon=1)
    assert isinstance(var, float)
    assert isinstance(es, float)
    assert es <= var + 1e-9


async def test_kupiec_test_no_violations_no_reject():
    rng = np.random.default_rng(14)
    returns = rng.normal(0.0, 0.01, 250)
    # Set VaR very conservative (very negative) so there are zero violations
    var = float(np.min(returns)) - 0.01
    result = ValueAtRisk._kupiec_test(returns, var, alpha=0.01)
    assert result["n_violations"] == 0
    assert result["reject"] is False


async def test_monte_carlo_var_returns_tuple():
    rng = np.random.default_rng(15)
    returns = rng.normal(0.0, 0.02, 250)
    var, es = ValueAtRisk._monte_carlo_var(returns, alpha=0.01, horizon=1, n_sims=1000)
    assert isinstance(var, float)
    assert isinstance(es, float)
    assert es <= var + 1e-9
