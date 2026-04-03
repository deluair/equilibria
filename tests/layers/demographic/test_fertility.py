"""Tests for L17 Demographic: FertilityEconomics module."""

from __future__ import annotations

import pytest
from app.layers.demographic.fertility import FertilityEconomics
from tests.layers.demographic.conftest import _insert_wdi_series, _insert_points


# ---------------------------------------------------------------------------
# Static tests
# ---------------------------------------------------------------------------

def test_instantiation():
    assert FertilityEconomics() is not None


def test_layer_id():
    assert FertilityEconomics.layer_id == "l17"


def test_name():
    assert FertilityEconomics().name == "Fertility Economics"


def test_classify_transition_stage_1():
    stage = FertilityEconomics._classify_transition_stage(cbr=35, cdr=25, tfr=5.5)
    assert stage == 1


def test_classify_transition_stage_5():
    stage = FertilityEconomics._classify_transition_stage(cbr=10, cdr=10, tfr=1.4)
    assert stage == 5


def test_tfr_to_score_near_replacement():
    score = FertilityEconomics._tfr_to_score(2.1)
    assert score == 15.0


def test_tfr_to_score_sub_replacement():
    score = FertilityEconomics._tfr_to_score(1.0)
    assert score > 45


def test_tfr_to_score_very_high():
    score = FertilityEconomics._tfr_to_score(6.0)
    assert score >= 40


# ---------------------------------------------------------------------------
# Async / DB-backed tests
# ---------------------------------------------------------------------------

async def test_compute_empty_db_returns_error_dict(db_conn):
    result = await FertilityEconomics().compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)
    assert "score" in result


async def test_compute_empty_db_score_is_50(db_conn):
    result = await FertilityEconomics().compute(db_conn, country_iso3="BGD")
    assert result["score"] == 50


async def test_compute_with_tfr_data(db_conn):
    country = "BGD"
    tfr_id = await _insert_wdi_series(db_conn, "SP.DYN.TFRT.IN", country)
    years = [f"{y}-01-01" for y in range(1990, 2021)]
    # Declining TFR from 4.0 to 2.0
    values = [4.0 - (i / len(years)) * 2.0 for i in range(len(years))]
    await _insert_points(db_conn, tfr_id, list(zip(years, values)))

    result = await FertilityEconomics().compute(db_conn, country_iso3=country)
    assert isinstance(result, dict)
    assert 0 <= result["score"] <= 100
    tfr_trend = result["results"].get("tfr_trend")
    if tfr_trend:
        assert "latest_tfr" in tfr_trend
        assert tfr_trend["declining"] is True


async def test_compute_with_demographic_transition_data(db_conn):
    country = "KOR"
    tfr_id = await _insert_wdi_series(db_conn, "SP.DYN.TFRT.IN", country)
    cbr_id = await _insert_wdi_series(db_conn, "SP.DYN.CBRT.IN", country)
    cdr_id = await _insert_wdi_series(db_conn, "SP.DYN.CDRT.IN", country)

    years = [f"{y}-01-01" for y in range(2010, 2022)]
    await _insert_points(db_conn, tfr_id, [(y, 1.2) for y in years])
    await _insert_points(db_conn, cbr_id, [(y, 9.0) for y in years])
    await _insert_points(db_conn, cdr_id, [(y, 8.0) for y in years])

    result = await FertilityEconomics().compute(db_conn, country_iso3=country)
    assert isinstance(result, dict)
    transition = result["results"].get("demographic_transition")
    if transition:
        assert transition["stage"] in {1, 2, 3, 4, 5}
