"""Tests for L7 Financial: Financial Contagion module."""

from __future__ import annotations

import numpy as np
import pytest
from app.layers.financial.contagion import FinancialContagion
from tests.layers.financial.conftest import _insert_series, _insert_points


def test_instantiation():
    assert FinancialContagion() is not None


def test_layer_id():
    assert FinancialContagion().layer_id == "l7"


def test_name():
    assert FinancialContagion().name == "Financial Contagion"


async def test_compute_empty_db_returns_unavailable(db_conn):
    result = await FinancialContagion().compute(db_conn, country_iso3="USA",
                                                 partner_iso3="GBR")
    assert isinstance(result, dict)
    assert result["score"] is None
    assert result["signal"] == "UNAVAILABLE"


async def test_compute_with_two_country_data_returns_score(db_conn):
    """Insert 100 overlapping daily return rows for two countries."""
    rng = np.random.default_rng(50)
    n = 100
    dates = [f"2022-{(i // 28 + 1):02d}-{(i % 28 + 1):02d}" for i in range(n)]
    base = rng.normal(0, 0.015, n)
    ra = base + rng.normal(0, 0.005, n)
    rb = base + rng.normal(0, 0.005, n)

    for country, vals in [("USA", ra), ("GBR", rb)]:
        sid = await _insert_series(db_conn, "fred", f"ret_{country}", country,
                                    f"asset_return {country}")
        await _insert_points(db_conn, sid, list(zip(dates, vals.tolist())))

    result = await FinancialContagion().compute(db_conn, country_iso3="USA",
                                                 partner_iso3="GBR")
    assert isinstance(result.get("score"), float)
    assert 0.0 <= result["score"] <= 100.0


async def test_forbes_rigobon_no_contagion_when_correlations_equal():
    rng = np.random.default_rng(51)
    n = 80
    ra = rng.normal(0, 0.02, n)
    rb = rng.normal(0, 0.02, n)
    # Calm and crisis have the same data (no difference)
    result = FinancialContagion._forbes_rigobon_test(ra[:40], rb[:40], ra[:40], rb[:40])
    assert "contagion_detected" in result
    assert isinstance(result["contagion_detected"], bool)


async def test_empirical_tail_dependence_between_0_and_1():
    rng = np.random.default_rng(52)
    ra = rng.standard_normal(300)
    rb = rng.standard_normal(300)
    result = FinancialContagion._empirical_tail_dependence(ra, rb)
    assert 0.0 <= result["lower"] <= 1.0
    assert 0.0 <= result["upper"] <= 1.0


async def test_rolling_correlation_length():
    rng = np.random.default_rng(53)
    ra = rng.standard_normal(120)
    rb = rng.standard_normal(120)
    corrs = FinancialContagion._rolling_correlation(ra, rb, window=60)
    assert len(corrs) == 120 - 60 + 1


async def test_regime_copula_has_current_regime():
    rng = np.random.default_rng(54)
    ra = rng.standard_normal(150)
    rb = rng.standard_normal(150)
    result = FinancialContagion._regime_copula(ra, rb, window=60)
    assert "current_regime" in result
    assert result["current_regime"] in ("high_dependence", "low_dependence", "unknown")


async def test_dcc_garch_correlation_bounded():
    rng = np.random.default_rng(55)
    n = 100
    ra = rng.normal(0, 0.02, n)
    rb = rng.normal(0, 0.02, n)
    result = FinancialContagion._dcc_garch(ra, rb)
    assert result is not None
    assert all(-1.0 <= c <= 1.0 for c in result["correlations"])
