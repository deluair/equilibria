"""Tests for FREDCollector (instantiation, validate, series constants)."""

import pytest

from app.collectors.fred import FRED_SERIES, SERIES_META, FREDCollector


# ---------------------------------------------------------------------------
# Constants / metadata integrity
# ---------------------------------------------------------------------------

def test_fred_series_count():
    """FRED_SERIES should define exactly 30 series."""
    assert len(FRED_SERIES) == 30


def test_fred_series_meta_coverage():
    """SERIES_META should cover every canonical name in FRED_SERIES."""
    for canonical in FRED_SERIES:
        assert canonical in SERIES_META, f"Missing metadata for {canonical}"


def test_fred_series_ids_non_empty():
    """Every FRED series ID must be a non-empty string."""
    for name, sid in FRED_SERIES.items():
        assert isinstance(sid, str) and sid, f"Empty series ID for {name}"


def test_series_meta_has_unit_and_frequency():
    """Every metadata entry must have 'unit' and 'frequency'."""
    for name, meta in SERIES_META.items():
        assert "unit" in meta, f"Missing 'unit' for {name}"
        assert "frequency" in meta, f"Missing 'frequency' for {name}"


# ---------------------------------------------------------------------------
# Instantiation: requires FRED_API_KEY
# ---------------------------------------------------------------------------

def test_fred_collector_raises_without_api_key(monkeypatch):
    """FREDCollector raises ValueError when FRED_API_KEY is not set."""
    from app.config import settings
    monkeypatch.setattr(settings, "fred_api_key", "")
    with pytest.raises(ValueError, match="FRED_API_KEY"):
        FREDCollector()


def test_fred_collector_instantiates_with_key(monkeypatch):
    """FREDCollector instantiates successfully when FRED_API_KEY is set."""
    from unittest.mock import MagicMock

    import fredapi

    from app.config import settings
    monkeypatch.setattr(settings, "fred_api_key", "fake_key")
    # Patch the Fred client so no real HTTP call happens
    monkeypatch.setattr(fredapi, "Fred", lambda api_key: MagicMock())
    c = FREDCollector()
    assert c.name == "fred"


# ---------------------------------------------------------------------------
# Validate
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fred_validate_drops_nan(monkeypatch):
    """FREDCollector.validate() drops rows with NaN values."""
    from unittest.mock import MagicMock

    import fredapi

    from app.config import settings
    monkeypatch.setattr(settings, "fred_api_key", "fake_key")
    monkeypatch.setattr(fredapi, "Fred", lambda api_key: MagicMock())

    c = FREDCollector()
    rows = [
        {"value": 1.5, "date": "2023-01-01"},
        {"value": float("nan"), "date": "2023-02-01"},
        {"value": 2.0, "date": "2023-03-01"},
    ]
    valid = await c.validate(rows)
    assert len(valid) == 2
    assert all(r["value"] == r["value"] for r in valid)  # no NaN


@pytest.mark.asyncio
async def test_fred_validate_drops_non_numeric(monkeypatch):
    """FREDCollector.validate() drops rows where value cannot be cast to float."""
    from unittest.mock import MagicMock

    import fredapi

    from app.config import settings
    monkeypatch.setattr(settings, "fred_api_key", "fake_key")
    monkeypatch.setattr(fredapi, "Fred", lambda api_key: MagicMock())

    c = FREDCollector()
    rows = [
        {"value": ".", "date": "2023-01-01"},
        {"value": None, "date": "2023-02-01"},
        {"value": 3.14, "date": "2023-03-01"},
    ]
    valid = await c.validate(rows)
    assert len(valid) == 1
    assert valid[0]["value"] == 3.14
