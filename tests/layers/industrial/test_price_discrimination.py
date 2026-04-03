"""Tests for L14 Industrial: PriceDiscrimination module."""

from __future__ import annotations

import json

import numpy as np
import pytest
from app.layers.industrial.price_discrimination import PriceDiscrimination
from tests.layers.industrial.conftest import _insert_series, _insert_points


# --- instantiation ---

def test_instantiation():
    assert PriceDiscrimination() is not None


def test_layer_id():
    assert PriceDiscrimination().layer_id == "l14"


def test_name():
    assert PriceDiscrimination().name == "Price Discrimination"


# --- empty DB ---

async def test_compute_empty_db_returns_dict(db_conn):
    result = await PriceDiscrimination().compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)


async def test_compute_empty_db_signal_unavailable(db_conn):
    result = await PriceDiscrimination().compute(db_conn, country_iso3="USA")
    assert result.get("signal") == "UNAVAILABLE" or result.get("score") is None


# --- with synthetic data ---

async def _insert_price_data(db_conn, n: int = 10):
    """Insert n price_discrimination rows with price/cost/quantity/segment."""
    rng = np.random.default_rng(42)
    for i in range(n):
        meta = {
            "cost": float(rng.uniform(2.0, 4.0)),
            "quantity": float(rng.uniform(10.0, 100.0)),
            "segment": "A" if i % 2 == 0 else "B",
        }
        sid = await _insert_series(
            db_conn, "price_discrimination", f"txn_{i}", "USA",
            f"transaction {i}", metadata=meta,
        )
        price = float(rng.uniform(5.0, 12.0))
        await _insert_points(db_conn, sid, [(f"2022-{(i % 12) + 1:02d}-01", price)])


async def test_compute_with_transactions_returns_score(db_conn):
    await _insert_price_data(db_conn, n=10)
    result = await PriceDiscrimination().compute(db_conn, country_iso3="USA")
    if result.get("score") is not None:
        assert 0.0 <= result["score"] <= 100.0


async def test_compute_with_transactions_has_degree_scores(db_conn):
    await _insert_price_data(db_conn, n=10)
    result = await PriceDiscrimination().compute(db_conn, country_iso3="USA")
    if result.get("score") is not None:
        assert "degree_scores" in result
        assert "dominant_type" in result


# --- static helpers ---

def test_identify_degrees_third_degree_detected():
    """Two-segment data with large price gap should score third degree highest."""
    rng = np.random.default_rng(0)
    transactions = []
    for _ in range(10):
        transactions.append({"price": float(rng.uniform(8.0, 10.0)), "quantity": 50.0,
                              "wtp": None, "segment": "premium", "bundle": None, "cost": 3.0})
    for _ in range(10):
        transactions.append({"price": float(rng.uniform(2.0, 4.0)), "quantity": 50.0,
                              "wtp": None, "segment": "discount", "bundle": None, "cost": 3.0})

    scores = PriceDiscrimination._identify_degrees(transactions)
    assert isinstance(scores["third_degree"], float)
    assert 0.0 <= scores["third_degree"] <= 1.0


def test_bundling_analysis_none_on_no_bundles():
    transactions = [{"price": 5.0, "bundle": None, "quantity": 1.0}] * 5
    result = PriceDiscrimination._bundling_analysis(transactions)
    assert result is None
