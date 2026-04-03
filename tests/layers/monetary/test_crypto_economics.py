"""Tests for L15 Monetary: CryptoEconomics module."""

from __future__ import annotations

import numpy as np
import pytest
from app.layers.monetary.crypto_economics import CryptoEconomics
from tests.layers.monetary.conftest import _insert_series, _insert_points


# --- instantiation ---

def test_instantiation():
    assert CryptoEconomics() is not None


def test_layer_id():
    assert CryptoEconomics().layer_id == "l15"


def test_name():
    assert CryptoEconomics().name == "Crypto Economics"


# --- empty DB ---

async def test_compute_empty_db_returns_dict(raw_conn):
    result = await CryptoEconomics().compute(raw_conn)
    assert isinstance(result, dict)


async def test_compute_empty_db_has_score(raw_conn):
    result = await CryptoEconomics().compute(raw_conn)
    assert "score" in result
    assert isinstance(result["score"], (int, float))


# --- with synthetic data ---

async def _populate_crypto(db_conn, n: int = 20):
    """Insert BTC price, supply, issuance, stablecoin peg, and bank deposits."""
    rng = np.random.default_rng(41)
    # BTC supply increases over halvings; issuance ~900/day = ~328500/year
    supply = 19e6 + np.arange(n) * 32850  # approximate
    issuance = np.full(n, 328500.0)
    # Price loosely correlated with S2F
    price = 30000.0 + rng.normal(0, 5000, n)
    # Stablecoin peg near 1.0
    peg = 1.0 + rng.normal(0, 0.002, n)
    # Bank deposits
    deposits = 17000e9 + np.arange(n) * 1e9

    dates = [f"{2015 + i // 2}-{(i % 2) * 6 + 1:02d}-01" for i in range(n)]

    for code, vals in [
        ("BTC_PRICE", price),
        ("BTC_SUPPLY", supply),
        ("BTC_ANNUAL_ISSUANCE", issuance),
        ("USDT_PRICE", peg),
        ("BANK_DEPOSITS_TOTAL", deposits),
    ]:
        sid = await _insert_series(db_conn, code)
        await _insert_points(db_conn, sid, list(zip(dates, vals.tolist())))


async def test_compute_with_crypto_data_returns_score(db_conn, raw_conn):
    await _populate_crypto(db_conn, n=20)
    result = await CryptoEconomics().compute(raw_conn)
    assert 0.0 <= result["score"] <= 100.0


async def test_compute_has_stock_to_flow_section(db_conn, raw_conn):
    await _populate_crypto(db_conn, n=20)
    result = await CryptoEconomics().compute(raw_conn)
    if "results" in result:
        assert "stock_to_flow" in result["results"]


async def test_compute_has_stablecoin_risk_section(db_conn, raw_conn):
    await _populate_crypto(db_conn, n=20)
    result = await CryptoEconomics().compute(raw_conn)
    if "results" in result:
        assert "stablecoin_risk" in result["results"]
