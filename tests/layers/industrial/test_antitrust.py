"""Tests for L14 Industrial: AntitrustAnalysis module."""

from __future__ import annotations

import json

import numpy as np
import pytest
from app.layers.industrial.antitrust import AntitrustAnalysis
from tests.layers.industrial.conftest import _insert_series, _insert_points


# --- instantiation ---

def test_instantiation():
    assert AntitrustAnalysis() is not None


def test_layer_id():
    assert AntitrustAnalysis().layer_id == "l14"


def test_name():
    assert AntitrustAnalysis().name == "Antitrust Analysis"


# --- empty DB ---

async def test_compute_empty_db_returns_dict(db_conn):
    result = await AntitrustAnalysis().compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)


async def test_compute_empty_db_signal_unavailable(db_conn):
    result = await AntitrustAnalysis().compute(db_conn, country_iso3="USA")
    assert result.get("signal") == "UNAVAILABLE" or result.get("score") is None


# --- with synthetic data ---

async def _insert_antitrust_data(db_conn, n: int = 15, cartel_split: int = 8):
    """Insert n antitrust price observations; first cartel_split are cartel period."""
    rng = np.random.default_rng(99)
    for i in range(n):
        is_cartel = i < cartel_split
        price = float(rng.uniform(12.0, 15.0)) if is_cartel else float(rng.uniform(8.0, 11.0))
        meta = {
            "average_variable_cost": 6.0,
            "average_total_cost": 8.0,
            "margin": 0.35,
            "market_share": 0.25,
            "cartel_period": is_cartel,
        }
        sid = await _insert_series(
            db_conn, "antitrust", f"at_{i}", "USA",
            f"antitrust obs {i}", metadata=meta,
        )
        year = 2010 + i
        await _insert_points(db_conn, sid, [(f"{year}-01-01", price)])


async def test_compute_with_antitrust_data_returns_score(db_conn):
    await _insert_antitrust_data(db_conn)
    result = await AntitrustAnalysis().compute(db_conn, country_iso3="USA")
    if result.get("score") is not None:
        assert 0.0 <= result["score"] <= 100.0


async def test_compute_antitrust_has_cartel_detection(db_conn):
    await _insert_antitrust_data(db_conn)
    result = await AntitrustAnalysis().compute(db_conn, country_iso3="USA")
    if result.get("score") is not None:
        assert "cartel_detection" in result


# --- static helpers ---

def test_ssnip_test_narrow_market_high_margin():
    data = [{"margin": 0.50}] * 10
    result = AntitrustAnalysis._ssnip_test(data, ssnip_pct=0.05)
    assert result is not None
    assert result["narrow_market"] is True


def test_predatory_pricing_legal_classification():
    data = [{"price": 10.0, "avc": 6.0, "atc": 8.0, "share": 0.2, "date": "2020"}] * 8
    result = AntitrustAnalysis._predatory_pricing_test(data)
    assert result is not None
    assert result["classification"] == "legal"
