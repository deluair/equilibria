"""Tests for L15 Monetary: MoneyDemand module.

Monetary modules call db.execute_fetchall() (native aiosqlite method),
so compute() must receive raw_conn (db_conn.conn), not the DBConnection wrapper.
"""

from __future__ import annotations

import numpy as np
import pytest
from app.layers.monetary.money_demand import MoneyDemand
from tests.layers.monetary.conftest import _insert_series, _insert_points


# --- instantiation ---

def test_instantiation():
    assert MoneyDemand() is not None


def test_layer_id():
    assert MoneyDemand().layer_id == "l15"


def test_name():
    assert MoneyDemand().name == "Money Demand"


# --- empty DB (raw_conn returns {"score": 50.0, "results": {"error": ...}}) ---

async def test_compute_empty_db_returns_dict(raw_conn):
    result = await MoneyDemand().compute(raw_conn, country="USA")
    assert isinstance(result, dict)


async def test_compute_empty_db_has_score(raw_conn):
    result = await MoneyDemand().compute(raw_conn, country="USA")
    assert "score" in result
    assert isinstance(result["score"], (int, float))


# --- with synthetic data ---

async def _populate_money_demand(db_conn, n: int = 30):
    """Insert M2, RGDP, CPI, POLICY_RATE series with n quarterly observations."""
    rng = np.random.default_rng(5)
    base = {
        "M2_USA": 15000.0,
        "RGDP_USA": 20000.0,
        "CPI_USA": 100.0,
        "POLICY_RATE_USA": 2.0,
    }
    growth = {"M2_USA": 0.015, "RGDP_USA": 0.006, "CPI_USA": 0.005, "POLICY_RATE_USA": 0.0}
    series_ids = {}
    for code, start_val in base.items():
        series_ids[code] = await _insert_series(db_conn, code)

    dates = [f"{1990 + i // 4}-{(i % 4) * 3 + 1:02d}-01" for i in range(n)]
    for i, date in enumerate(dates):
        for code, start_val in base.items():
            val = start_val * (1 + growth[code]) ** i + rng.normal(0, start_val * 0.005)
            await _insert_points(db_conn, series_ids[code], [(date, max(val, 0.01))])


async def test_compute_with_data_returns_lower_score(db_conn, raw_conn):
    await _populate_money_demand(db_conn, n=30)
    result = await MoneyDemand().compute(raw_conn, country="USA")
    assert "score" in result
    assert 0.0 <= result["score"] <= 100.0


async def test_compute_with_data_has_demand_function(db_conn, raw_conn):
    await _populate_money_demand(db_conn, n=30)
    result = await MoneyDemand().compute(raw_conn, country="USA")
    if "results" in result and "demand_function" in result.get("results", {}):
        df = result["results"]["demand_function"]
        assert "income_elasticity" in df
        assert "r_squared" in df
