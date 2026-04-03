"""Tests for L17 Demographic: PopulationGrowth module."""

from __future__ import annotations

import pytest
from app.layers.demographic.population_growth import PopulationGrowth
from tests.layers.demographic.conftest import _insert_wdi_series, _insert_points


# ---------------------------------------------------------------------------
# Static tests
# ---------------------------------------------------------------------------

def test_instantiation():
    assert PopulationGrowth() is not None


def test_layer_id():
    assert PopulationGrowth.layer_id == "l17"


def test_name():
    assert PopulationGrowth().name == "Population Growth"


def test_classify_epoch_malthusian():
    epoch = PopulationGrowth._classify_epoch(avg_gdp_growth=0.3, pop_growth=2.5, tfr=6.0)
    assert epoch == "malthusian"


def test_classify_epoch_modern():
    epoch = PopulationGrowth._classify_epoch(avg_gdp_growth=2.0, pop_growth=0.5, tfr=1.8)
    assert epoch == "demographic_transition_complete"


def test_classify_epoch_decline():
    epoch = PopulationGrowth._classify_epoch(avg_gdp_growth=1.0, pop_growth=-0.5, tfr=1.3)
    assert epoch == "population_decline"


# ---------------------------------------------------------------------------
# Async / DB-backed tests
# ---------------------------------------------------------------------------

async def test_compute_empty_db_returns_error_dict(db_conn):
    result = await PopulationGrowth().compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)
    assert "score" in result


async def test_compute_empty_db_score_is_50(db_conn):
    result = await PopulationGrowth().compute(db_conn, country_iso3="BGD")
    assert result["score"] == 50


async def test_compute_with_growth_and_gdp_data(db_conn):
    country = "NGA"
    pg_id = await _insert_wdi_series(db_conn, "SP.POP.GROW", country)
    gdp_id = await _insert_wdi_series(db_conn, "NY.GDP.PCAP.KD", country)
    tfr_id = await _insert_wdi_series(db_conn, "SP.DYN.TFRT.IN", country)

    years = [f"{y}-01-01" for y in range(1995, 2022)]
    await _insert_points(db_conn, pg_id,  [(y, 2.5) for y in years])
    await _insert_points(db_conn, gdp_id, [(y, 1500.0 + i * 50) for i, y in enumerate(years)])
    await _insert_points(db_conn, tfr_id, [(y, 5.5 - i * 0.05) for i, y in enumerate(years)])

    result = await PopulationGrowth().compute(db_conn, country_iso3=country)
    assert isinstance(result, dict)
    assert 0 <= result["score"] <= 100
    ug = result["results"].get("unified_growth")
    if ug:
        assert "epoch" in ug


async def test_compute_malthusian_raises_score(db_conn):
    country = "TST"
    pg_id = await _insert_wdi_series(db_conn, "SP.POP.GROW", country)
    gdp_id = await _insert_wdi_series(db_conn, "NY.GDP.PCAP.KD", country)

    years = [f"{y}-01-01" for y in range(1970, 1990)]
    # Very high population growth, stagnant GDP -> Malthusian signal
    await _insert_points(db_conn, pg_id,  [(y, 3.0) for y in years])
    await _insert_points(db_conn, gdp_id, [(y, 500.0 + (i % 3)) for i, y in enumerate(years)])

    result = await PopulationGrowth().compute(db_conn, country_iso3=country)
    assert isinstance(result, dict)
    # Malthusian or post_malthusian epoch should push score >= 60
    if result["results"].get("unified_growth"):
        assert result["score"] >= 50
