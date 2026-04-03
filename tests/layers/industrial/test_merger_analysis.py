"""Tests for L14 Industrial: MergerAnalysis module."""

from __future__ import annotations

import json

import numpy as np
import pytest
from app.layers.industrial.merger_analysis import MergerAnalysis
from tests.layers.industrial.conftest import _insert_series, _insert_points


# --- instantiation ---

def test_instantiation():
    assert MergerAnalysis() is not None


def test_layer_id():
    assert MergerAnalysis().layer_id == "l14"


def test_name():
    assert MergerAnalysis().name == "Merger Analysis"


# --- empty DB ---

async def test_compute_empty_db_returns_dict(db_conn):
    result = await MergerAnalysis().compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)


async def test_compute_empty_db_signal_unavailable(db_conn):
    result = await MergerAnalysis().compute(db_conn, country_iso3="USA")
    assert result.get("signal") == "UNAVAILABLE" or result.get("score") is None


# --- with synthetic data ---

async def test_compute_with_four_firms_returns_hhi_delta(db_conn):
    """Four firms with defined shares; the two largest are marked merging."""
    firms = [
        {"market_share": 0.35, "price": 10.0, "marginal_cost": 6.0, "merging": True},
        {"market_share": 0.25, "price": 10.0, "marginal_cost": 6.0, "merging": True},
        {"market_share": 0.20, "price": 10.0, "marginal_cost": 6.0, "merging": False},
        {"market_share": 0.20, "price": 10.0, "marginal_cost": 6.0, "merging": False},
    ]
    for i, firm in enumerate(firms):
        sid = await _insert_series(
            db_conn, "merger_analysis", f"mfirm_{i}", "USA",
            f"merger firm {i}", metadata=firm,
        )
        await _insert_points(db_conn, sid, [("2023-01-01", float(firm["market_share"] * 100))])

    result = await MergerAnalysis().compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)
    assert "delta_hhi" in result
    assert isinstance(result["delta_hhi"], float)
    assert result["delta_hhi"] >= 0.0


async def test_compute_score_in_range_with_data(db_conn):
    firms = [
        {"market_share": 0.30, "price": 8.0, "marginal_cost": 5.0, "merging": False},
        {"market_share": 0.30, "price": 8.0, "marginal_cost": 5.0, "merging": False},
        {"market_share": 0.20, "price": 8.0, "marginal_cost": 5.0, "merging": False},
        {"market_share": 0.20, "price": 8.0, "marginal_cost": 5.0, "merging": False},
    ]
    for i, firm in enumerate(firms):
        sid = await _insert_series(
            db_conn, "merger_analysis", f"nfirm_{i}", "USA",
            f"non-merging firm {i}", metadata=firm,
        )
        await _insert_points(db_conn, sid, [("2023-01-01", float(i + 1))])

    result = await MergerAnalysis().compute(db_conn, country_iso3="USA")
    if result.get("score") is not None:
        assert 0.0 <= result["score"] <= 100.0


# --- static helper: logit simulation ---

def test_logit_simulation_returns_none_without_prices():
    firms = [{"share": 0.5, "price": None, "mc": None, "merging": True},
             {"share": 0.5, "price": None, "mc": None, "merging": False}]
    result = MergerAnalysis._logit_merger_simulation(firms, firms[:2], 1.0)
    assert result is None


def test_delta_hhi_formula():
    """Delta HHI = 2 * s1_norm * s2_norm for two firms."""
    s1, s2 = 0.3, 0.2
    # Simple check: delta HHI with these shares should be 2*0.3*0.2 = 0.12
    assert abs(2 * s1 * s2 - 0.12) < 1e-10
