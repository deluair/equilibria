"""Tests for L7 Financial: CAPM module."""

from __future__ import annotations

import numpy as np
import pytest
from app.layers.financial.capm import CAPM
from tests.layers.financial.conftest import _insert_series, _insert_points


def test_instantiation():
    assert CAPM() is not None


def test_layer_id():
    assert CAPM().layer_id == "l7"


def test_name():
    assert CAPM().name == "CAPM"


async def test_compute_empty_db_returns_unavailable(db_conn):
    result = await CAPM().compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)
    assert result["score"] is None
    assert result["signal"] == "UNAVAILABLE"


async def test_compute_with_sufficient_data_returns_score(db_conn):
    """Insert 30 monthly asset and market return rows to exceed the 24-row minimum."""
    rng = np.random.default_rng(0)
    market_returns = rng.normal(0.008, 0.04, 30)
    asset_returns = 0.5 + 1.2 * market_returns + rng.normal(0, 0.01, 30)

    mkt_id = await _insert_series(db_conn, "fred", "mkt_ret", "USA",
                                   "market_return sp500 monthly")
    asset_id = await _insert_series(db_conn, "fred", "asset_ret", "USA",
                                    "asset_return market_index monthly")
    rf_id = await _insert_series(db_conn, "fred", "rf_rate", "USA",
                                  "risk_free tbill monthly")

    dates = [f"202{y}-{m:02d}-01" for y in range(2, 5) for m in range(1, 11)][:30]
    await _insert_points(db_conn, mkt_id, list(zip(dates, market_returns.tolist())))
    await _insert_points(db_conn, asset_id, list(zip(dates, asset_returns.tolist())))
    await _insert_points(db_conn, rf_id, list(zip(dates, [0.002] * 30)))

    result = await CAPM().compute(db_conn, country_iso3="USA", asset_id="market_index")
    assert isinstance(result.get("score"), float)
    assert 0.0 <= result["score"] <= 100.0


async def test_classify_series_asset_match():
    assert CAPM._classify_series("asset_return market_index", "market_index") == "asset"


async def test_classify_series_market():
    assert CAPM._classify_series("market_return sp500", "other") == "market"


async def test_classify_series_rf():
    assert CAPM._classify_series("risk_free tbill 3mo", "x") == "rf"


async def test_ols_regression_known_values():
    """beta=2, alpha=0.01 by construction."""
    rng = np.random.default_rng(1)
    x = rng.normal(0, 1, 100)
    y = 0.01 + 2.0 * x + rng.normal(0, 0.05, 100)
    res = CAPM._ols_regression(x, y)
    assert abs(res["beta"] - 2.0) < 0.15
    assert abs(res["alpha"] - 0.01) < 0.05
    assert 0.0 <= res["r_squared"] <= 1.0


async def test_ols_regression_residuals_shape():
    rng = np.random.default_rng(2)
    x = rng.standard_normal(50)
    y = x + rng.standard_normal(50)
    res = CAPM._ols_regression(x, y)
    assert len(res["residuals"]) == 50
