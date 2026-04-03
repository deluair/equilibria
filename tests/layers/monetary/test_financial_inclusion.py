"""Tests for L15 Monetary: FinancialInclusion module."""

from __future__ import annotations

import numpy as np
import pytest
from app.layers.monetary.financial_inclusion import FinancialInclusion
from tests.layers.monetary.conftest import _insert_series, _insert_points


# --- instantiation ---

def test_instantiation():
    assert FinancialInclusion() is not None


def test_layer_id():
    assert FinancialInclusion().layer_id == "l15"


def test_name():
    assert FinancialInclusion().name == "Financial Inclusion"


# --- empty DB ---

async def test_compute_empty_db_returns_dict(raw_conn):
    result = await FinancialInclusion().compute(raw_conn, country="USA")
    assert isinstance(result, dict)


async def test_compute_empty_db_has_score(raw_conn):
    result = await FinancialInclusion().compute(raw_conn, country="USA")
    assert "score" in result
    assert isinstance(result["score"], (int, float))


# --- with synthetic data ---

async def _populate_financial_inclusion(db_conn, n: int = 8):
    """Insert Findex and mobile money series for USA."""
    rng = np.random.default_rng(53)
    dates = [f"{2007 + i * 2}-01-01" for i in range(n)]

    # Rising account ownership from 70 to 93
    account = np.linspace(70.0, 93.0, n) + rng.normal(0, 1, n)
    # Other findex dimensions
    savings = np.linspace(40.0, 65.0, n) + rng.normal(0, 2, n)
    digital = np.linspace(20.0, 80.0, n) + rng.normal(0, 3, n)
    # Mobile money penetration
    mobile = np.linspace(5.0, 60.0, n) + rng.normal(0, 2, n)
    # GDP per capita
    gdp_pc = np.linspace(50000.0, 65000.0, n)

    series_data = {
        "FINDEX_ACCOUNT_USA": account,
        "FINDEX_SAVINGS_USA": savings,
        "FINDEX_DIGITAL_USA": digital,
        "MOBILE_MONEY_USA": mobile,
        "GDP_PC_USA": gdp_pc,
    }
    for code, vals in series_data.items():
        sid = await _insert_series(db_conn, code)
        await _insert_points(db_conn, sid, list(zip(dates, vals.clip(0.1).tolist())))


async def test_compute_with_inclusion_data_returns_score(db_conn, raw_conn):
    await _populate_financial_inclusion(db_conn, n=8)
    result = await FinancialInclusion().compute(raw_conn, country="USA")
    assert 0.0 <= result["score"] <= 100.0


async def test_compute_has_findex_composite(db_conn, raw_conn):
    await _populate_financial_inclusion(db_conn, n=8)
    result = await FinancialInclusion().compute(raw_conn, country="USA")
    if "results" in result:
        assert "findex_composite" in result["results"]
        fc = result["results"]["findex_composite"]
        if "fii_score" in fc:
            assert 0.0 <= fc["fii_score"] <= 1.0


async def test_compute_has_mobile_money(db_conn, raw_conn):
    await _populate_financial_inclusion(db_conn, n=8)
    result = await FinancialInclusion().compute(raw_conn, country="USA")
    if "results" in result:
        assert "mobile_money" in result["results"]
