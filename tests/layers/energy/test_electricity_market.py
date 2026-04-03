"""Tests for L16 Energy: ElectricityMarket module."""

from __future__ import annotations

import pytest
from app.layers.energy.electricity_market import ElectricityMarket
from tests.layers.energy.conftest import _insert_series, _insert_points


# ---------------------------------------------------------------------------
# Static tests
# ---------------------------------------------------------------------------

def test_instantiation():
    assert ElectricityMarket() is not None


def test_layer_id():
    assert ElectricityMarket.layer_id == "l16"


def test_name():
    assert ElectricityMarket().name == "Electricity Market"


# ---------------------------------------------------------------------------
# Async / DB-backed tests
# ---------------------------------------------------------------------------

async def test_compute_empty_db_returns_dict(db_conn):
    result = await ElectricityMarket().compute(db_conn, country="USA")
    assert isinstance(result, dict)


async def test_compute_empty_db_score_in_range(db_conn):
    result = await ElectricityMarket().compute(db_conn, country="USA")
    assert "score" in result
    assert 0 <= result["score"] <= 100


async def test_compute_results_country_key(db_conn):
    result = await ElectricityMarket().compute(db_conn, country="DEU")
    assert result["results"]["country"] == "DEU"


async def test_compute_with_capacity_and_demand(db_conn):
    country = "USA"
    dates = [f"{y}-01-01" for y in range(2015, 2024)]

    # Insert capacity technologies and demand so merit-order block fires.
    for tech in ["nuclear", "coal", "gas", "wind", "solar"]:
        sid = await _insert_series(db_conn, f"CAPACITY_{tech.upper()}_{country}", country)
        await _insert_points(db_conn, sid, [(d, 50_000.0) for d in dates])

    demand_id = await _insert_series(db_conn, f"ELECTRICITY_DEMAND_{country}", country)
    await _insert_points(db_conn, demand_id, [(d, 120_000.0) for d in dates])

    peak_id = await _insert_series(db_conn, f"PEAK_DEMAND_{country}", country)
    await _insert_points(db_conn, peak_id, [(d, 140_000.0) for d in dates])

    installed_id = await _insert_series(db_conn, f"INSTALLED_CAPACITY_{country}", country)
    await _insert_points(db_conn, installed_id, [(d, 300_000.0) for d in dates])

    result = await ElectricityMarket().compute(db_conn, country=country)
    assert isinstance(result, dict)
    assert 0 <= result["score"] <= 100
    mo = result["results"].get("merit_order")
    if mo:
        assert "clearing_price" in mo
        assert "total_capacity_mw" in mo
