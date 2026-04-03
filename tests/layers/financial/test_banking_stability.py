"""Tests for L7 Financial: Banking Stability module."""

from __future__ import annotations

import numpy as np
import pytest
from app.layers.financial.banking_stability import BankingStability
from tests.layers.financial.conftest import _insert_series, _insert_points


def test_instantiation():
    assert BankingStability() is not None


def test_layer_id():
    assert BankingStability().layer_id == "l7"


def test_name():
    assert BankingStability().name == "Banking Stability"


async def test_compute_empty_db_returns_unavailable(db_conn):
    result = await BankingStability().compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)
    assert result["score"] is None
    assert result["signal"] == "UNAVAILABLE"


async def test_compute_with_banking_data_returns_score(db_conn):
    dates = [f"201{y}-01-01" for y in range(0, 10)]
    roa_vals = [1.2, 1.1, 1.3, 1.0, 0.9, 1.1, 1.2, 1.15, 1.05, 1.1]
    eq_vals = [10.5] * 10
    npl_vals = [4.5, 5.0, 5.5, 6.0, 5.8, 5.2, 4.8, 4.5, 4.2, 4.0]
    car_vals = [11.0] * 10

    series_map = [
        ("return_on_assets roa", roa_vals),
        ("equity_assets equity_ratio", eq_vals),
        ("npl nonperforming ratio", npl_vals),
        ("capital_adequacy car tier1", car_vals),
    ]

    for desc, vals in series_map:
        sid = await _insert_series(db_conn, "wdi", desc.replace(" ", "_"),
                                    "BGD", desc)
        await _insert_points(db_conn, sid, list(zip(dates, vals)))

    result = await BankingStability().compute(db_conn, country_iso3="BGD")
    assert isinstance(result.get("score"), float)
    assert 0.0 <= result["score"] <= 100.0


async def test_z_score_interpretation_stable():
    assert BankingStability._z_score_interpretation(35.0) == "very stable"


async def test_z_score_interpretation_high_distress():
    assert BankingStability._z_score_interpretation(3.0) == "high distress risk"


async def test_z_score_interpretation_none():
    assert BankingStability._z_score_interpretation(None) == "unavailable"


async def test_extract_latest_finds_by_keyword():
    series = {"return_on_assets roa monthly": [("2023-01", 1.2), ("2023-02", 1.3)]}
    result = BankingStability._extract_latest(series, ["roa"])
    assert result == 1.3


async def test_early_warning_no_data_returns_normal():
    result = BankingStability._early_warning_system({}, z_score=None,
                                                      npl_ratio=None, car=None)
    assert result["alert_level"] == "normal"
    assert result["flags_triggered"] == 0


async def test_early_warning_bad_indicators_flags():
    result = BankingStability._early_warning_system(
        {}, z_score=3.0, npl_ratio=12.0, car=6.0
    )
    assert result["flags_triggered"] >= 2
    assert result["alert_level"] in ("elevated", "critical")


async def test_dkd_insufficient_indicators_returns_none():
    # Only 1 indicator available
    series = {"gdp_growth real_gdp": [("2020", 3.5)]}
    result = BankingStability()._dkd_crisis_probability(series)
    assert result is None
