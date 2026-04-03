"""Tests for L16 Energy: FossilSubsidy module."""

from __future__ import annotations

import pytest
from app.layers.energy.fossil_subsidy import FossilSubsidy
from tests.layers.energy.conftest import _insert_series, _insert_points


# ---------------------------------------------------------------------------
# Static tests
# ---------------------------------------------------------------------------

def test_instantiation():
    assert FossilSubsidy() is not None


def test_layer_id():
    assert FossilSubsidy.layer_id == "l16"


def test_name():
    assert FossilSubsidy().name == "Fossil Fuel Subsidy"


# ---------------------------------------------------------------------------
# Async / DB-backed tests
# ---------------------------------------------------------------------------

async def test_compute_empty_db_returns_dict(db_conn):
    result = await FossilSubsidy().compute(db_conn, country="USA")
    assert isinstance(result, dict)


async def test_compute_empty_db_score_in_range(db_conn):
    result = await FossilSubsidy().compute(db_conn, country="USA")
    assert "score" in result
    assert 0 <= result["score"] <= 100


async def test_compute_country_in_results(db_conn):
    result = await FossilSubsidy().compute(db_conn, country="IRN")
    assert result["results"]["country"] == "IRN"


async def test_compute_with_oil_price_gap(db_conn):
    country = "USA"
    dates = [f"{y}-01-01" for y in range(2010, 2023)]

    cons_id = await _insert_series(db_conn, f"OIL_CONSUMPTION_{country}", country)
    ref_id = await _insert_series(db_conn, f"OIL_REFERENCE_PRICE_{country}", country)
    con_id = await _insert_series(db_conn, f"OIL_CONSUMER_PRICE_{country}", country)
    gdp_id = await _insert_series(db_conn, f"GDP_{country}", country)

    # Consumer price below reference => pre-tax subsidy > 0
    await _insert_points(db_conn, cons_id, [(d, 1000.0) for d in dates])
    await _insert_points(db_conn, ref_id,  [(d, 80.0) for d in dates])
    await _insert_points(db_conn, con_id,  [(d, 60.0) for d in dates])
    await _insert_points(db_conn, gdp_id,  [(d, 2.0e13) for d in dates])

    result = await FossilSubsidy().compute(db_conn, country=country)
    assert isinstance(result, dict)
    assert 0 <= result["score"] <= 100
    sub = result["results"].get("subsidies")
    if sub:
        assert sub["pre_tax_total"] > 0
        assert "by_fuel" in sub
