"""Tests for L17 Demographic: AgingEconomics module."""

from __future__ import annotations

import pytest
from app.layers.demographic.aging import AgingEconomics
from tests.layers.demographic.conftest import _insert_wdi_series, _insert_points


# ---------------------------------------------------------------------------
# Static tests
# ---------------------------------------------------------------------------

def test_instantiation():
    assert AgingEconomics() is not None


def test_layer_id():
    assert AgingEconomics.layer_id == "l17"


def test_name():
    assert AgingEconomics().name == "Aging Economics"


def test_dependency_to_score_low():
    # Low dependency (<10%) => score 10
    assert AgingEconomics._dependency_to_score(8.0) == 10.0


def test_dependency_to_score_high():
    # Dependency at 40% should be in crisis range (>65)
    score = AgingEconomics._dependency_to_score(40.0)
    assert score >= 65


def test_dependency_to_score_very_high():
    # Dependency above 45% caps near 100
    score = AgingEconomics._dependency_to_score(50.0)
    assert score >= 90


# ---------------------------------------------------------------------------
# Async / DB-backed tests
# ---------------------------------------------------------------------------

async def test_compute_empty_db_returns_error_dict(db_conn):
    result = await AgingEconomics().compute(db_conn, country_iso3="JPN")
    assert isinstance(result, dict)
    assert "score" in result


async def test_compute_empty_db_score_is_50(db_conn):
    result = await AgingEconomics().compute(db_conn, country_iso3="JPN")
    assert result["score"] == 50


async def test_compute_with_dependency_ratio_data(db_conn):
    country = "JPN"
    dep_id = await _insert_wdi_series(db_conn, "SP.POP.DPND.OL", country)
    years = [f"{y}-01-01" for y in range(1990, 2022)]
    # Japan-like aging: dependency rising from 17 to 48
    values = [17.0 + (i / len(years)) * 31.0 for i in range(len(years))]
    await _insert_points(db_conn, dep_id, list(zip(years, values)))

    result = await AgingEconomics().compute(db_conn, country_iso3=country)
    assert isinstance(result, dict)
    assert 0 <= result["score"] <= 100
    dep = result["results"].get("dependency_ratio")
    if dep:
        assert dep["aging_accelerating"]  # True or np.True_
        assert "projected_10yr" in dep


async def test_compute_with_aaron_condition_data(db_conn):
    country = "DEU"
    dep_id = await _insert_wdi_series(db_conn, "SP.POP.DPND.OL", country)
    gdp_g_id = await _insert_wdi_series(db_conn, "NY.GDP.PCAP.KD.ZG", country)
    pop_g_id = await _insert_wdi_series(db_conn, "SP.POP.GROW", country)

    years = [f"{y}-01-01" for y in range(2005, 2023)]
    await _insert_points(db_conn, dep_id,   [(y, 32.0) for y in years])
    await _insert_points(db_conn, gdp_g_id, [(y, 1.5) for y in years])
    await _insert_points(db_conn, pop_g_id, [(y, 0.2) for y in years])

    result = await AgingEconomics().compute(db_conn, country_iso3=country)
    assert isinstance(result, dict)
    assert 0 <= result["score"] <= 100
    aaron = result["results"].get("aaron_condition")
    if aaron:
        assert "aaron_favors_paygo" in aaron
        assert "support_ratio" in aaron
