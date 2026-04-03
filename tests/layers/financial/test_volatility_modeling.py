"""Tests for L7 Financial: Volatility Modeling module."""

from __future__ import annotations

import numpy as np
import pytest
from app.layers.financial.volatility_modeling import VolatilityModeling
from tests.layers.financial.conftest import _insert_series, _insert_points


def test_instantiation():
    assert VolatilityModeling() is not None


def test_layer_id():
    assert VolatilityModeling().layer_id == "l7"


def test_name():
    assert VolatilityModeling().name == "Volatility Modeling"


async def test_compute_empty_db_returns_unavailable(db_conn):
    result = await VolatilityModeling().compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)
    assert result["score"] is None
    assert result["signal"] == "UNAVAILABLE"


async def test_compute_with_sufficient_data_returns_score(db_conn):
    """100 daily returns exceed the 60-row minimum."""
    rng = np.random.default_rng(40)
    returns = rng.normal(0.0, 0.015, 100)
    dates = [f"2022-{(i // 28 + 1):02d}-{(i % 28 + 1):02d}" for i in range(100)]
    sid = await _insert_series(db_conn, "yahoo", "vol_idx", "USA",
                                "market_index asset_returns daily")
    await _insert_points(db_conn, sid, list(zip(dates, returns.tolist())))

    result = await VolatilityModeling().compute(db_conn, country_iso3="USA",
                                                 asset_id="vol_idx")
    assert isinstance(result.get("score"), float)
    assert 0.0 <= result["score"] <= 100.0


async def test_fit_garch_stationarity():
    """alpha + beta < 1 is enforced by the optimizer bounds."""
    rng = np.random.default_rng(41)
    eps = rng.normal(0, 0.015, 200)
    result = VolatilityModeling._fit_garch(eps)
    assert result is not None
    assert result["alpha"] + result["beta"] < 1.0


async def test_fit_garch_parameters_positive():
    rng = np.random.default_rng(42)
    eps = rng.normal(0, 0.015, 150)
    result = VolatilityModeling._fit_garch(eps)
    assert result is not None
    assert result["omega"] > 0
    assert result["alpha"] > 0
    assert result["beta"] > 0


async def test_garch_forecast_length():
    omega, alpha, beta = 1e-6, 0.08, 0.88
    sigma2_last = 0.0003
    forecasts = VolatilityModeling._garch_forecast(omega, alpha, beta, sigma2_last, horizon=10)
    assert len(forecasts) == 10
    for v in forecasts:
        assert v >= 0.0


async def test_rolling_realized_vol_length():
    rng = np.random.default_rng(43)
    returns = rng.normal(0, 0.01, 60)
    rv = VolatilityModeling._rolling_realized_vol(returns, window=22)
    assert len(rv) == 60 - 22 + 1


async def test_fit_gjr_garch_has_gamma():
    rng = np.random.default_rng(44)
    eps = rng.normal(0, 0.015, 150)
    result = VolatilityModeling._fit_gjr_garch(eps)
    assert result is not None
    assert "gamma" in result
