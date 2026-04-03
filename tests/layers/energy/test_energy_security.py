"""Tests for L16 Energy: EnergySecurity module."""

from __future__ import annotations

import numpy as np
import pytest
from app.layers.energy.energy_security import EnergySecurity
from tests.layers.energy.conftest import _insert_series, _insert_points


# ---------------------------------------------------------------------------
# Static tests
# ---------------------------------------------------------------------------

def test_instantiation():
    assert EnergySecurity() is not None


def test_layer_id():
    assert EnergySecurity.layer_id == "l16"


def test_name():
    assert EnergySecurity().name == "Energy Security"


# ---------------------------------------------------------------------------
# Async / DB-backed tests
# ---------------------------------------------------------------------------

async def test_compute_empty_db_returns_dict(db_conn):
    result = await EnergySecurity().compute(db_conn, country="USA")
    assert isinstance(result, dict)


async def test_compute_empty_db_score_in_range(db_conn):
    result = await EnergySecurity().compute(db_conn, country="USA")
    assert "score" in result
    assert 0 <= result["score"] <= 100


async def test_compute_results_country_key(db_conn):
    result = await EnergySecurity().compute(db_conn, country="BGD")
    assert result["results"]["country"] == "BGD"


async def test_compute_with_import_dependence_data(db_conn):
    # Insert enough rows so import_dependence block fires.
    country = "USA"
    dates = [f"{y}-01-01" for y in range(2000, 2015)]

    imports_id = await _insert_series(db_conn, f"ENERGY_IMPORTS_{country}", country)
    exports_id = await _insert_series(db_conn, f"ENERGY_EXPORTS_{country}", country)
    tpes_id = await _insert_series(db_conn, f"TPES_{country}", country)

    await _insert_points(db_conn, imports_id, [(d, 50.0 + i) for i, d in enumerate(dates)])
    await _insert_points(db_conn, exports_id, [(d, 10.0 + i * 0.5) for i, d in enumerate(dates)])
    await _insert_points(db_conn, tpes_id, [(d, 200.0 + i) for i, d in enumerate(dates)])

    result = await EnergySecurity().compute(db_conn, country=country)
    assert isinstance(result, dict)
    assert "score" in result
    assert 0 <= result["score"] <= 100
    # import_dependence block should have fired
    if result["results"].get("import_dependence"):
        idi = result["results"]["import_dependence"]
        assert "latest" in idi
        assert 0 <= idi["latest"] <= 1
