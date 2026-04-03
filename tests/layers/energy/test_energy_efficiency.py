"""Tests for L16 Energy: EnergyEfficiency module."""

from __future__ import annotations

import numpy as np
import pytest
from app.layers.energy.energy_efficiency import EnergyEfficiency, _log_mean


# ---------------------------------------------------------------------------
# Static / unit tests
# ---------------------------------------------------------------------------

def test_instantiation():
    assert EnergyEfficiency() is not None


def test_layer_id():
    assert EnergyEfficiency.layer_id == "l16"


def test_name():
    assert EnergyEfficiency().name == "Energy Efficiency"


def test_log_mean_equal_inputs():
    # When a == b, L(a, b) == a
    assert _log_mean(5.0, 5.0) == 5.0


def test_log_mean_zero_input():
    # Zero or negative inputs return 0
    assert _log_mean(0.0, 5.0) == 0.0
    assert _log_mean(5.0, 0.0) == 0.0


def test_log_mean_positive():
    val = _log_mean(4.0, 2.0)
    # (4-2) / ln(4/2) = 2 / ln(2) ~ 2.885
    assert abs(val - 2.0 / np.log(2.0)) < 1e-9


# ---------------------------------------------------------------------------
# Async / DB-backed tests
# ---------------------------------------------------------------------------

async def test_compute_empty_db_returns_dict(db_conn):
    result = await EnergyEfficiency().compute(db_conn, country="USA")
    assert isinstance(result, dict)


async def test_compute_empty_db_score_in_range(db_conn):
    result = await EnergyEfficiency().compute(db_conn, country="USA")
    assert "score" in result
    assert 0 <= result["score"] <= 100


async def test_compute_results_country_key(db_conn):
    result = await EnergyEfficiency().compute(db_conn, country="JPN")
    assert result["results"]["country"] == "JPN"


async def test_compute_no_exception_default_kwargs(db_conn):
    # Should not raise; just return a score.
    result = await EnergyEfficiency().compute(db_conn)
    assert isinstance(result["score"], float)
