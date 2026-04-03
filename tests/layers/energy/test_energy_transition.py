"""Tests for L16 Energy: EnergyTransition module."""

from __future__ import annotations

import numpy as np
import pytest
from app.layers.energy.energy_transition import EnergyTransition, _bass_cumulative, _fit_bass
from tests.layers.energy.conftest import _insert_series, _insert_points


# ---------------------------------------------------------------------------
# Static / unit tests
# ---------------------------------------------------------------------------

def test_instantiation():
    assert EnergyTransition() is not None


def test_layer_id():
    assert EnergyTransition.layer_id == "l16"


def test_name():
    assert EnergyTransition().name == "Energy Transition"


def test_bass_cumulative_at_zero():
    # At t=0, cumulative adoption should be near 0.
    result = _bass_cumulative(np.array([0.0]), p=0.03, q=0.4, m=1000.0)
    assert 0 <= float(result[0]) < 100


def test_bass_cumulative_approaches_market_potential():
    # At large t, adoption approaches m.
    result = _bass_cumulative(np.array([500.0]), p=0.03, q=0.4, m=1000.0)
    assert float(result[0]) > 950


def test_fit_bass_insufficient_data():
    # Fewer than 5 points returns None.
    result = _fit_bass(np.array([0.0, 1.0, 2.0]), np.array([10.0, 50.0, 80.0]), 200.0)
    assert result is None


# ---------------------------------------------------------------------------
# Async / DB-backed tests
# ---------------------------------------------------------------------------

async def test_compute_empty_db_returns_dict(db_conn):
    result = await EnergyTransition().compute(db_conn, country="USA")
    assert isinstance(result, dict)


async def test_compute_empty_db_score_in_range(db_conn):
    result = await EnergyTransition().compute(db_conn, country="USA")
    assert "score" in result
    assert 0 <= result["score"] <= 100


async def test_compute_results_country_key(db_conn):
    result = await EnergyTransition().compute(db_conn, country="DEU")
    assert result["results"]["country"] == "DEU"


async def test_compute_with_investment_gap_data(db_conn):
    country = "USA"
    dates = [f"{y}-01-01" for y in range(2018, 2024)]

    inv_id = await _insert_series(db_conn, f"CLEAN_ENERGY_INVESTMENT_{country}", country)
    req_id = await _insert_series(db_conn, f"NZE_REQUIRED_INVESTMENT_{country}", country)

    await _insert_points(db_conn, inv_id, [(d, 300.0 + i * 20) for i, d in enumerate(dates)])
    await _insert_points(db_conn, req_id, [(d, 800.0) for d in dates])

    result = await EnergyTransition().compute(db_conn, country=country)
    assert isinstance(result, dict)
    assert 0 <= result["score"] <= 100
    inv_gap = result["results"].get("investment_gap")
    if inv_gap:
        assert inv_gap["gap_bn"] > 0
        assert "on_track" in inv_gap
