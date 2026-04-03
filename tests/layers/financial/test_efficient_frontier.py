"""Tests for L7 Financial: Efficient Frontier module."""

from __future__ import annotations

import numpy as np
import pytest
from app.layers.financial.efficient_frontier import EfficientFrontier
from tests.layers.financial.conftest import _insert_series, _insert_points


def test_instantiation():
    assert EfficientFrontier() is not None


def test_layer_id():
    assert EfficientFrontier().layer_id == "l7"


def test_name():
    assert EfficientFrontier().name == "Efficient Frontier"


async def test_compute_empty_db_returns_unavailable(db_conn):
    result = await EfficientFrontier().compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)
    assert result["score"] is None
    assert result["signal"] == "UNAVAILABLE"


async def test_compute_single_asset_returns_unavailable(db_conn):
    """Only one asset: optimizer needs >= 2."""
    rng = np.random.default_rng(3)
    dates = [f"2020-{m:02d}-01" for m in range(1, 13)]
    sid = await _insert_series(db_conn, "fred", "asset_a", "USA", "asset_a_return")
    await _insert_points(db_conn, sid, list(zip(dates, rng.normal(0.01, 0.04, 12).tolist())))

    result = await EfficientFrontier().compute(db_conn, country_iso3="USA")
    assert result["score"] is None


async def test_compute_two_assets_returns_score(db_conn):
    rng = np.random.default_rng(4)
    dates = [f"20{y:02d}-{m:02d}-01" for y in range(20, 21) for m in range(1, 13)]
    for tag in ("equities", "bonds"):
        sid = await _insert_series(db_conn, "fred", f"ret_{tag}", "GBR", f"{tag}_return")
        await _insert_points(db_conn, sid, list(zip(dates, rng.normal(0.005, 0.03, 12).tolist())))

    result = await EfficientFrontier().compute(db_conn, country_iso3="GBR")
    assert isinstance(result.get("score"), float)
    assert 0.0 <= result["score"] <= 100.0


async def test_min_variance_portfolio_weights_sum_to_one():
    rng = np.random.default_rng(5)
    n = 4
    mu = rng.uniform(0.005, 0.02, n)
    A = rng.standard_normal((n, n))
    cov = A.T @ A / n + np.eye(n) * 0.001
    result = EfficientFrontier._min_variance_portfolio(mu, cov, n)
    assert abs(result["weights"].sum() - 1.0) < 1e-4
    assert result["vol"] >= 0.0


async def test_tangent_portfolio_weights_sum_to_one():
    rng = np.random.default_rng(6)
    n = 3
    mu = rng.uniform(0.005, 0.02, n)
    A = rng.standard_normal((n, n))
    cov = A.T @ A / n + np.eye(n) * 0.001
    result = EfficientFrontier._tangent_portfolio(mu, cov, n, rf=0.02)
    assert abs(result["weights"].sum() - 1.0) < 1e-4


async def test_black_litterman_returns_posterior(db_conn):
    rng = np.random.default_rng(7)
    n = 3
    mu = rng.uniform(0.005, 0.02, n)
    A = rng.standard_normal((n, n))
    cov = A.T @ A / n + np.eye(n) * 0.001
    views = [{"asset_idx": 0, "view_return": 0.015, "confidence": 0.8}]
    result = EfficientFrontier._black_litterman(mu, cov, n, rf=0.02, views=views)
    assert "posterior_returns" in result
    assert len(result["posterior_returns"]) == n


async def test_frontier_points_are_positive_vol(db_conn):
    rng = np.random.default_rng(8)
    n = 2
    mu = np.array([0.05, 0.10])
    A = rng.standard_normal((n, n))
    cov = A.T @ A / n + np.eye(n) * 0.002
    pts = EfficientFrontier._compute_frontier(mu, cov, n, n_points=5, rf=0.02)
    for pt in pts:
        assert pt["volatility"] >= 0.0
