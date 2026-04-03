"""Tests for L17 Demographic: HumanCapitalAccumulation module."""

from __future__ import annotations

import pytest
from app.layers.demographic.human_capital import HumanCapitalAccumulation
from tests.layers.demographic.conftest import _insert_wdi_series, _insert_points


# ---------------------------------------------------------------------------
# Static tests
# ---------------------------------------------------------------------------

def test_instantiation():
    assert HumanCapitalAccumulation() is not None


def test_layer_id():
    assert HumanCapitalAccumulation.layer_id == "l17"


def test_name():
    assert HumanCapitalAccumulation().name == "Human Capital Accumulation"


# ---------------------------------------------------------------------------
# Async / DB-backed tests
# ---------------------------------------------------------------------------

async def test_compute_empty_db_returns_error_dict(db_conn):
    result = await HumanCapitalAccumulation().compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)
    assert "score" in result


async def test_compute_empty_db_score_is_50(db_conn):
    result = await HumanCapitalAccumulation().compute(db_conn, country_iso3="BGD")
    assert result["score"] == 50


async def test_compute_with_enrollment_data(db_conn):
    country = "KOR"
    sec_id = await _insert_wdi_series(db_conn, "SE.SEC.ENRR", country)
    years = [f"{y}-01-01" for y in range(2000, 2022)]
    # High and rising secondary enrollment
    await _insert_points(db_conn, sec_id, [(y, 90.0 + i * 0.1) for i, y in enumerate(years)])

    result = await HumanCapitalAccumulation().compute(db_conn, country_iso3=country)
    assert isinstance(result, dict)
    assert 0 <= result["score"] <= 100
    profile = result["results"].get("investment_profile")
    if profile:
        assert "secondary_enrollment" in profile


async def test_compute_score_reduced_by_high_enrollment(db_conn):
    country = "FIN"
    sec_id = await _insert_wdi_series(db_conn, "SE.SEC.ENRR", country)
    edu_id = await _insert_wdi_series(db_conn, "SE.XPD.TOTL.GD.ZS", country)
    le_id = await _insert_wdi_series(db_conn, "SP.DYN.LE00.IN", country)

    years = [f"{y}-01-01" for y in range(2005, 2022)]
    await _insert_points(db_conn, sec_id, [(y, 95.0) for y in years])
    await _insert_points(db_conn, edu_id, [(y, 6.5) for y in years])
    await _insert_points(db_conn, le_id,  [(y, 82.0) for y in years])

    result = await HumanCapitalAccumulation().compute(db_conn, country_iso3=country)
    assert isinstance(result, dict)
    # High enrollment, spending, and LE should reduce score from baseline 50
    assert result["score"] < 50
