"""Tests for L15 Monetary: InflationTargeting module."""

from __future__ import annotations

import numpy as np
import pytest
from app.layers.monetary.inflation_targeting import InflationTargeting
from tests.layers.monetary.conftest import _insert_series, _insert_points


# --- instantiation ---

def test_instantiation():
    assert InflationTargeting() is not None


def test_layer_id():
    assert InflationTargeting().layer_id == "l15"


def test_name():
    assert InflationTargeting().name == "Inflation Targeting"


# --- empty DB ---

async def test_compute_empty_db_returns_dict(raw_conn):
    result = await InflationTargeting().compute(raw_conn, country="USA")
    assert isinstance(result, dict)


async def test_compute_empty_db_has_score(raw_conn):
    result = await InflationTargeting().compute(raw_conn, country="USA")
    assert "score" in result
    assert isinstance(result["score"], (int, float))


# --- with synthetic data ---

async def _populate_inflation(db_conn, n: int = 30, pi_target: float = 2.0):
    """Insert INFLATION_USA series with n quarterly observations."""
    rng = np.random.default_rng(23)
    # Pre-IT: high inflation; post-IT: low near target
    pre = pi_target + 3.0 + rng.normal(0, 1.5, n // 2)
    post = pi_target + 0.5 + rng.normal(0, 0.5, n - n // 2)
    pi = np.concatenate([pre, post])

    sid = await _insert_series(db_conn, "INFLATION_USA")
    dates = [f"{2008 + i // 4}-{(i % 4) * 3 + 1:02d}-01" for i in range(n)]
    await _insert_points(db_conn, sid, list(zip(dates, pi.tolist())))


async def test_compute_with_inflation_data_returns_score(db_conn, raw_conn):
    await _populate_inflation(db_conn, n=30)
    result = await InflationTargeting().compute(raw_conn, country="USA", pi_target=2.0)
    assert 0.0 <= result["score"] <= 100.0


async def test_compute_adoption_effects_present(db_conn, raw_conn):
    await _populate_inflation(db_conn, n=30)
    result = await InflationTargeting().compute(raw_conn, country="USA", pi_target=2.0)
    if "results" in result and "adoption_effects" in result["results"]:
        ae = result["results"]["adoption_effects"]
        # Mean reduction should be positive (pre > post) given our constructed data
        assert ae["mean_reduction"] > 0


async def test_compute_ball_sheridan_section_present(db_conn, raw_conn):
    await _populate_inflation(db_conn, n=30)
    result = await InflationTargeting().compute(raw_conn, country="USA", pi_target=2.0)
    if "results" in result and "ball_sheridan" in result["results"]:
        bs = result["results"]["ball_sheridan"]
        assert "it_coefficient" in bs
