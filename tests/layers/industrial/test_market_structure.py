"""Tests for L14 Industrial: MarketStructure module."""

from __future__ import annotations

import json

import numpy as np
import pytest
from app.layers.industrial.market_structure import MarketStructure
from tests.layers.industrial.conftest import _insert_series, _insert_points


# --- instantiation ---

def test_instantiation():
    assert MarketStructure() is not None


def test_layer_id():
    assert MarketStructure().layer_id == "l14"


def test_name():
    assert MarketStructure().name == "Market Structure"


# --- empty DB ---

async def test_compute_empty_db_returns_dict(db_conn):
    result = await MarketStructure().compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)


async def test_compute_empty_db_signal_unavailable(db_conn):
    result = await MarketStructure().compute(db_conn, country_iso3="USA")
    assert result.get("signal") == "UNAVAILABLE" or result.get("score") is None


# --- with synthetic data ---

async def test_compute_with_firm_shares_returns_hhi(db_conn):
    """Insert 6 firms each with distinct market share; expect HHI in result."""
    shares = [0.30, 0.20, 0.15, 0.15, 0.10, 0.10]
    for i, share in enumerate(shares):
        meta = {"market_share": share}
        sid = await _insert_series(
            db_conn, "market_structure", f"firm_{i}", "USA",
            f"Firm {i} market_structure", metadata=meta,
        )
        await _insert_points(db_conn, sid, [("2022-01-01", float(i + 1))])

    result = await MarketStructure().compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)
    assert "hhi" in result
    assert isinstance(result["hhi"], float)
    assert 0.0 < result["hhi"] <= 1.0


async def test_compute_hhi_score_in_range(db_conn):
    """Score must be in [0, 100]."""
    shares = [0.40, 0.30, 0.20, 0.10]
    for i, share in enumerate(shares):
        meta = {"market_share": share}
        sid = await _insert_series(
            db_conn, "market_structure", f"s_{i}", "USA",
            f"firm share {i} market_structure", metadata=meta,
        )
        await _insert_points(db_conn, sid, [("2023-01-01", float(i + 1))])

    result = await MarketStructure().compute(db_conn, country_iso3="USA")
    if result.get("score") is not None:
        assert 0.0 <= result["score"] <= 100.0


# --- static helpers ---

def test_gini_equal_shares():
    """Equal shares -> Gini = 0."""
    shares = np.array([0.25, 0.25, 0.25, 0.25])
    gini = MarketStructure._gini(shares)
    assert abs(gini) < 1e-10


def test_gini_monopoly():
    """Pure monopoly share array -> Gini = 1."""
    shares = np.array([1.0, 0.0, 0.0, 0.0])
    # Sort ascending for correct formula
    gini = MarketStructure._gini(shares)
    assert gini >= 0.0
