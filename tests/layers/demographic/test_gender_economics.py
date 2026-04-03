"""Tests for L17 Demographic: GenderEconomics module."""

from __future__ import annotations

import pytest
from app.layers.demographic.gender_economics import GenderEconomics
from tests.layers.demographic.conftest import _insert_wdi_series, _insert_points


# ---------------------------------------------------------------------------
# Static tests
# ---------------------------------------------------------------------------

def test_instantiation():
    assert GenderEconomics() is not None


def test_layer_id():
    assert GenderEconomics.layer_id == "l17"


def test_name():
    assert GenderEconomics().name == "Gender Economics"


# ---------------------------------------------------------------------------
# Async / DB-backed tests
# ---------------------------------------------------------------------------

async def test_compute_empty_db_returns_error_dict(db_conn):
    result = await GenderEconomics().compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)
    assert "score" in result


async def test_compute_empty_db_score_is_50(db_conn):
    result = await GenderEconomics().compute(db_conn, country_iso3="BGD")
    assert result["score"] == 50


async def test_compute_with_lfpr_data(db_conn):
    country = "BGD"
    flfp_id = await _insert_wdi_series(db_conn, "SL.TLF.CACT.FE.ZS", country)
    mlfp_id = await _insert_wdi_series(db_conn, "SL.TLF.CACT.MA.ZS", country)

    years = [f"{y}-01-01" for y in range(2000, 2022)]
    await _insert_points(db_conn, flfp_id, [(y, 36.0 + i * 0.2) for i, y in enumerate(years)])
    await _insert_points(db_conn, mlfp_id, [(y, 83.0 - i * 0.1) for i, y in enumerate(years)])

    result = await GenderEconomics().compute(db_conn, country_iso3=country)
    assert isinstance(result, dict)
    assert 0 <= result["score"] <= 100
    gap = result["results"].get("gender_participation_gap")
    if gap:
        assert "participation_gap_pp" in gap
        assert gap["participation_gap_pp"] > 0


async def test_compute_large_gap_raises_score(db_conn):
    country = "YEM"
    flfp_id = await _insert_wdi_series(db_conn, "SL.TLF.CACT.FE.ZS", country)
    mlfp_id = await _insert_wdi_series(db_conn, "SL.TLF.CACT.MA.ZS", country)

    years = [f"{y}-01-01" for y in range(2010, 2022)]
    # Very large gap: female 6%, male 72%
    await _insert_points(db_conn, flfp_id, [(y, 6.0) for y in years])
    await _insert_points(db_conn, mlfp_id, [(y, 72.0) for y in years])

    result = await GenderEconomics().compute(db_conn, country_iso3=country)
    assert isinstance(result, dict)
    # Gap of 66 pp should push score >= 60
    assert result["score"] >= 60
