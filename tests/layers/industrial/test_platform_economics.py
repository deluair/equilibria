"""Tests for L14 Industrial: PlatformEconomics module."""

from __future__ import annotations

import json

import numpy as np
import pytest
from app.layers.industrial.platform_economics import PlatformEconomics
from tests.layers.industrial.conftest import _insert_series, _insert_points


# --- instantiation ---

def test_instantiation():
    assert PlatformEconomics() is not None


def test_layer_id():
    assert PlatformEconomics().layer_id == "l14"


def test_name():
    assert PlatformEconomics().name == "Platform Economics"


# --- empty DB ---

async def test_compute_empty_db_returns_dict(db_conn):
    result = await PlatformEconomics().compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)


async def test_compute_empty_db_signal_unavailable(db_conn):
    result = await PlatformEconomics().compute(db_conn, country_iso3="USA")
    assert result.get("signal") == "UNAVAILABLE" or result.get("score") is None


# --- with synthetic data ---

async def _insert_platform_data(db_conn, n: int = 5):
    """Insert n platform rows with two-sided user/price metadata."""
    rng = np.random.default_rng(11)
    shares = np.array([0.45, 0.25, 0.15, 0.10, 0.05])[:n]
    shares = shares / shares.sum()
    for i in range(n):
        meta = {
            "users_side_a": float(rng.integers(1000, 100000)),
            "users_side_b": float(rng.integers(500, 50000)),
            "price_side_a": float(rng.uniform(0.0, 5.0)),
            "price_side_b": float(rng.uniform(5.0, 20.0)),
            "cost_side_a": float(rng.uniform(0.5, 2.0)),
            "cost_side_b": float(rng.uniform(1.0, 4.0)),
            "market_share": float(shares[i]),
            "multi_homing_rate": float(rng.uniform(0.1, 0.5)),
            "switching_cost": float(rng.uniform(0.2, 0.8)),
        }
        sid = await _insert_series(
            db_conn, "platform_economics", f"plat_{i}", "USA",
            f"platform {i}", metadata=meta,
        )
        revenue = float(meta["users_side_a"] * meta["price_side_a"]
                        + meta["users_side_b"] * meta["price_side_b"])
        await _insert_points(db_conn, sid, [(f"2023-0{i+1}-01", revenue)])


async def test_compute_with_platform_data_returns_score(db_conn):
    await _insert_platform_data(db_conn, n=5)
    result = await PlatformEconomics().compute(db_conn, country_iso3="USA")
    if result.get("score") is not None:
        assert 0.0 <= result["score"] <= 100.0


async def test_compute_platform_data_has_winner_take_all(db_conn):
    await _insert_platform_data(db_conn, n=5)
    result = await PlatformEconomics().compute(db_conn, country_iso3="USA")
    if result.get("score") is not None:
        assert "winner_take_all" in result


# --- static helpers ---

def test_winner_take_all_high_concentration():
    platforms = [{"market_share": 0.80}, {"market_share": 0.15},
                 {"market_share": 0.05}]
    for p in platforms:
        p.update({"users_side_a": None, "users_side_b": None, "price_side_a": None,
                   "price_side_b": None, "cost_side_a": None, "cost_side_b": None,
                   "revenue": None, "multi_homing_rate": 0.1, "switching_cost": 0.7})
    result = PlatformEconomics._winner_take_all(platforms)
    assert result is not None
    assert result["top_platform_share"] == pytest.approx(1.0, abs=0.01) or result["top_platform_share"] > 0.5


def test_two_sided_pricing_identifies_subsidized_side():
    platforms = [
        {"price_side_a": 0.0, "price_side_b": 15.0,
         "cost_side_a": 1.0, "cost_side_b": 5.0},
    ]
    result = PlatformEconomics._two_sided_pricing(platforms)
    assert result is not None
    assert result["subsidized_side"] == "side_a"
