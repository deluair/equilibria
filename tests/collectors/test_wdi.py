"""Tests for WDICollector (instantiation, validate, constants)."""

import pytest

from app.collectors.wdi import COUNTRIES, WDI_INDICATORS, WDICollector


# ---------------------------------------------------------------------------
# Constants integrity
# ---------------------------------------------------------------------------

def test_wdi_indicators_count():
    """WDI_INDICATORS should define exactly 20 indicators."""
    assert len(WDI_INDICATORS) == 20


def test_wdi_countries_count():
    """COUNTRIES should map 50 ISO2 -> ISO3 pairs."""
    assert len(COUNTRIES) == 50


def test_wdi_indicator_tuples():
    """Each WDI_INDICATORS entry is a 3-tuple (canonical, unit, description)."""
    for code, entry in WDI_INDICATORS.items():
        assert len(entry) == 3, f"Expected 3-tuple for {code}"
        canonical, unit, desc = entry
        assert isinstance(canonical, str) and canonical
        assert isinstance(unit, str) and unit
        assert isinstance(desc, str) and desc


def test_wdi_iso2_keys_length():
    """All COUNTRIES keys are 2-character strings."""
    for iso2 in COUNTRIES:
        assert len(iso2) == 2, f"Invalid ISO2: {iso2}"


def test_wdi_iso3_values_length():
    """All COUNTRIES values are 3-character uppercase strings."""
    for iso3 in COUNTRIES.values():
        assert len(iso3) == 3
        assert iso3 == iso3.upper()


# ---------------------------------------------------------------------------
# Instantiation (no API key needed for WDI)
# ---------------------------------------------------------------------------

def test_wdi_collector_instantiation():
    """WDICollector instantiates without any API key requirement."""
    c = WDICollector()
    assert c.name == "wdi"


# ---------------------------------------------------------------------------
# Validate
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_wdi_validate_drops_nan():
    """WDICollector.validate() drops NaN values."""
    c = WDICollector()
    rows = [
        {"value": float("nan"), "country_iso3": "USA"},
        {"value": 3.5, "country_iso3": "USA"},
    ]
    valid = await c.validate(rows)
    assert len(valid) == 1
    assert valid[0]["value"] == 3.5


@pytest.mark.asyncio
async def test_wdi_validate_drops_unknown_country():
    """WDICollector.validate() drops rows with unknown ISO3 codes."""
    c = WDICollector()
    rows = [
        {"value": 1.0, "country_iso3": "ZZZ"},  # not in COUNTRIES
        {"value": 2.0, "country_iso3": "USA"},
    ]
    valid = await c.validate(rows)
    assert len(valid) == 1
    assert valid[0]["country_iso3"] == "USA"


@pytest.mark.asyncio
async def test_wdi_validate_drops_non_numeric():
    """WDICollector.validate() drops rows with non-numeric values."""
    c = WDICollector()
    rows = [
        {"value": None, "country_iso3": "DEU"},
        {"value": "n/a", "country_iso3": "DEU"},
        {"value": 5.0, "country_iso3": "DEU"},
    ]
    valid = await c.validate(rows)
    assert len(valid) == 1
    assert valid[0]["value"] == 5.0


@pytest.mark.asyncio
async def test_wdi_validate_empty_input():
    """WDICollector.validate() returns empty list for empty input."""
    c = WDICollector()
    valid = await c.validate([])
    assert valid == []
