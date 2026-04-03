"""Tests for L15 Monetary: CentralBankAnalysis module."""

from __future__ import annotations

import numpy as np
import pytest
from app.layers.monetary.central_bank import CentralBankAnalysis
from tests.layers.monetary.conftest import _insert_series, _insert_points


# --- instantiation ---

def test_instantiation():
    assert CentralBankAnalysis() is not None


def test_layer_id():
    assert CentralBankAnalysis().layer_id == "l15"


def test_name():
    assert CentralBankAnalysis().name == "Central Bank Analysis"


# --- empty DB ---

async def test_compute_empty_db_returns_dict(raw_conn):
    result = await CentralBankAnalysis().compute(raw_conn, country="USA")
    assert isinstance(result, dict)


async def test_compute_empty_db_has_score(raw_conn):
    result = await CentralBankAnalysis().compute(raw_conn, country="USA")
    assert "score" in result
    assert isinstance(result["score"], (int, float))


# --- with synthetic data ---

async def _populate_central_bank(db_conn, n: int = 30):
    """Insert POLICY_RATE, INFLATION, OUTPUT_GAP series."""
    rng = np.random.default_rng(17)
    codes = {
        "POLICY_RATE_USA": (3.0, 0.5),
        "INFLATION_USA": (2.5, 0.8),
        "OUTPUT_GAP_USA": (0.0, 1.5),
    }
    dates = [f"{2010 + i // 4}-{(i % 4) * 3 + 1:02d}-01" for i in range(n)]
    for code, (mean, std) in codes.items():
        sid = await _insert_series(db_conn, code)
        vals = mean + rng.normal(0, std, n)
        await _insert_points(db_conn, sid, list(zip(dates, vals.tolist())))


async def test_compute_with_data_returns_score_in_range(db_conn, raw_conn):
    await _populate_central_bank(db_conn, n=30)
    result = await CentralBankAnalysis().compute(raw_conn, country="USA")
    assert 0.0 <= result["score"] <= 100.0


async def test_compute_with_data_has_taylor_variants(db_conn, raw_conn):
    await _populate_central_bank(db_conn, n=30)
    result = await CentralBankAnalysis().compute(raw_conn, country="USA")
    if "results" in result and "taylor_variants" in result["results"]:
        tv = result["results"]["taylor_variants"]
        assert "estimated" in tv
        assert "taylor_principle_holds" in tv["estimated"]
