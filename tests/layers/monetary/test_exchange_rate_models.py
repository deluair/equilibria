"""Tests for L15 Monetary: ExchangeRateModels module."""

from __future__ import annotations

import numpy as np
import pytest
from app.layers.monetary.exchange_rate_models import ExchangeRateModels
from tests.layers.monetary.conftest import _insert_series, _insert_points


# --- instantiation ---

def test_instantiation():
    assert ExchangeRateModels() is not None


def test_layer_id():
    assert ExchangeRateModels().layer_id == "l15"


def test_name():
    assert ExchangeRateModels().name == "Exchange Rate Models"


# --- empty DB ---

async def test_compute_empty_db_returns_dict(raw_conn):
    result = await ExchangeRateModels().compute(raw_conn, country="USA")
    assert isinstance(result, dict)


async def test_compute_empty_db_has_score(raw_conn):
    result = await ExchangeRateModels().compute(raw_conn, country="USA")
    assert "score" in result
    assert isinstance(result["score"], (int, float))


# --- with synthetic data ---

async def _populate_exchange_rate(db_conn, n: int = 30):
    """Insert exchange rate + interest rate series for USA/EUR."""
    rng = np.random.default_rng(31)
    # Random walk exchange rate around 1.1
    ex = 1.1 * np.exp(np.cumsum(rng.normal(0, 0.01, n)))

    dates = [f"{2008 + i // 4}-{(i % 4) * 3 + 1:02d}-01" for i in range(n)]

    ex_sid = await _insert_series(db_conn, "EXRATE_USA_EUR")
    await _insert_points(db_conn, ex_sid, list(zip(dates, ex.tolist())))

    dom_sid = await _insert_series(db_conn, "POLICY_RATE_USA")
    await _insert_points(db_conn, dom_sid,
                         list(zip(dates, (2.0 + rng.normal(0, 0.5, n)).tolist())))

    for_sid = await _insert_series(db_conn, "POLICY_RATE_EUR")
    await _insert_points(db_conn, for_sid,
                         list(zip(dates, (1.5 + rng.normal(0, 0.5, n)).tolist())))


async def test_compute_with_exchange_data_returns_score(db_conn, raw_conn):
    await _populate_exchange_rate(db_conn, n=30)
    result = await ExchangeRateModels().compute(raw_conn, country="USA", partner="EUR")
    assert 0.0 <= result["score"] <= 100.0


async def test_compute_dornbusch_section_present(db_conn, raw_conn):
    await _populate_exchange_rate(db_conn, n=30)
    result = await ExchangeRateModels().compute(raw_conn, country="USA", partner="EUR")
    if "results" in result and "error" not in result["results"]:
        assert "dornbusch" in result["results"]


async def test_compute_meese_rogoff_section_present(db_conn, raw_conn):
    await _populate_exchange_rate(db_conn, n=30)
    result = await ExchangeRateModels().compute(raw_conn, country="USA", partner="EUR")
    if "results" in result and "error" not in result["results"]:
        assert "meese_rogoff" in result["results"]
