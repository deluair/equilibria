"""Tests for L17 Demographic: ChildDevelopment module."""

from __future__ import annotations

import pytest
from app.layers.demographic.child_development import ChildDevelopment
from tests.layers.demographic.conftest import _insert_wdi_series, _insert_points


# ---------------------------------------------------------------------------
# Static tests
# ---------------------------------------------------------------------------

def test_instantiation():
    assert ChildDevelopment() is not None


def test_layer_id():
    assert ChildDevelopment.layer_id == "l17"


def test_name():
    assert ChildDevelopment().name == "Child Development Economics"


# ---------------------------------------------------------------------------
# Async / DB-backed tests
# ---------------------------------------------------------------------------

async def test_compute_empty_db_returns_error_dict(db_conn):
    result = await ChildDevelopment().compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)
    assert "score" in result


async def test_compute_empty_db_score_is_50(db_conn):
    result = await ChildDevelopment().compute(db_conn, country_iso3="BGD")
    assert result["score"] == 50


async def test_compute_high_stunting_raises_score(db_conn):
    country = "ETH"
    stunt_id = await _insert_wdi_series(db_conn, "SH.STA.STNT.ZS", country)
    years = [f"{y}-01-01" for y in range(2005, 2020)]
    # High stunting ~45%
    await _insert_points(db_conn, stunt_id, [(y, 45.0) for y in years])

    result = await ChildDevelopment().compute(db_conn, country_iso3=country)
    assert isinstance(result, dict)
    assert 0 <= result["score"] <= 100
    # Stunting > 40 adds 25 to base 50 => score >= 75
    assert result["score"] >= 70


async def test_compute_with_full_profile(db_conn):
    country = "SWE"
    stunt_id = await _insert_wdi_series(db_conn, "SH.STA.STNT.ZS", country)
    prepri_id = await _insert_wdi_series(db_conn, "SE.PRE.ENRR", country)
    u5mr_id = await _insert_wdi_series(db_conn, "SH.DYN.MORT", country)
    edu_id = await _insert_wdi_series(db_conn, "SE.XPD.TOTL.GD.ZS", country)

    years = [f"{y}-01-01" for y in range(2005, 2022)]
    # Low stunting, high preschool, low u5mr => low stress score
    await _insert_points(db_conn, stunt_id,  [(y, 2.0) for y in years])
    await _insert_points(db_conn, prepri_id, [(y, 90.0) for y in years])
    await _insert_points(db_conn, u5mr_id,   [(y, 3.0) for y in years])
    await _insert_points(db_conn, edu_id,    [(y, 6.8) for y in years])

    result = await ChildDevelopment().compute(db_conn, country_iso3=country)
    assert isinstance(result, dict)
    assert 0 <= result["score"] <= 100
    # Low stunting + high preschool + low u5mr should produce a low score
    assert result["score"] <= 40
    profile = result["results"].get("country_profile")
    if profile:
        assert "stunting_pct" in profile
        assert "preprimary_enrollment" in profile
