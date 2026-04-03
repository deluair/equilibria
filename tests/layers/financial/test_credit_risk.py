"""Tests for L7 Financial: Credit Risk module."""

from __future__ import annotations

import numpy as np
import pytest
from app.layers.financial.credit_risk import CreditRisk
from tests.layers.financial.conftest import _insert_series, _insert_points


def test_instantiation():
    assert CreditRisk() is not None


def test_layer_id():
    assert CreditRisk().layer_id == "l7"


def test_name():
    assert CreditRisk().name == "Credit Risk"


async def test_compute_empty_db_returns_unavailable(db_conn):
    result = await CreditRisk().compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)
    assert result["score"] is None
    assert result["signal"] == "UNAVAILABLE"


async def test_compute_with_equity_debt_data_returns_score(db_conn):
    rng = np.random.default_rng(20)
    # Build a synthetic equity price series so sigma_e can be estimated
    prices = np.cumprod(1 + rng.normal(0.001, 0.02, 30)) * 100
    equity_vals = prices.tolist()
    debt_vals = [50.0] * 30
    rf_vals = [0.04] * 30

    dates = [f"202{y}-{m:02d}-01" for y in range(2, 5) for m in range(1, 11)][:30]

    eq_id = await _insert_series(db_conn, "fred", "eq_val", "USA", "equity market_cap stock")
    debt_id = await _insert_series(db_conn, "fred", "dbt", "USA", "debt liabilities face_value")
    rf_id = await _insert_series(db_conn, "fred", "rf_cr", "USA", "risk_free tbill rf")

    await _insert_points(db_conn, eq_id, list(zip(dates, equity_vals)))
    await _insert_points(db_conn, debt_id, list(zip(dates, debt_vals)))
    await _insert_points(db_conn, rf_id, list(zip(dates, rf_vals)))

    result = await CreditRisk().compute(db_conn, country_iso3="USA")
    assert isinstance(result.get("score"), float)
    assert 0.0 <= result["score"] <= 100.0


async def test_merton_model_pd_between_0_and_1():
    result = CreditRisk._merton_model(E=80.0, D=50.0, sigma_e=0.30, r=0.04, T=1.0)
    assert 0.0 <= result["pd"] <= 1.0
    assert result["V"] > 0.0


async def test_merton_model_high_leverage_high_pd():
    # Equity barely covers debt => high PD
    res_risky = CreditRisk._merton_model(E=5.0, D=100.0, sigma_e=0.50, r=0.04, T=1.0)
    res_safe = CreditRisk._merton_model(E=200.0, D=50.0, sigma_e=0.20, r=0.04, T=1.0)
    assert res_risky["pd"] > res_safe["pd"]


async def test_implied_credit_spread_positive():
    spread = CreditRisk._implied_credit_spread(pd=0.03, lgd=0.45, r=0.04, T=1.0)
    assert spread > 0.0


async def test_find_series_matches_keyword():
    series = {"equity market_cap stock": [1.0, 2.0], "other": [3.0]}
    result = CreditRisk._find_series(series, ["market_cap"])
    assert result == [1.0, 2.0]


async def test_estimate_transition_matrix_row_sums():
    rng = np.random.default_rng(21)
    ratings = rng.uniform(1, 8, 50).tolist()
    result = CreditRisk._estimate_transition_matrix(ratings, n_states=4)
    matrix = np.array(result["matrix"])
    row_sums = matrix.sum(axis=1)
    np.testing.assert_allclose(row_sums, np.ones(4), atol=1e-3)
