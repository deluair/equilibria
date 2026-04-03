"""Tests for L7 Financial: Term Structure module."""

from __future__ import annotations

import numpy as np
import pytest
from app.layers.financial.term_structure import TermStructure
from tests.layers.financial.conftest import _insert_series, _insert_points


def test_instantiation():
    assert TermStructure() is not None


def test_layer_id():
    assert TermStructure().layer_id == "l7"


def test_name():
    assert TermStructure().name == "Term Structure"


async def test_compute_empty_db_returns_unavailable(db_conn):
    result = await TermStructure().compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)
    assert result["score"] is None
    assert result["signal"] == "UNAVAILABLE"


async def test_parse_maturity_months():
    assert abs(TermStructure._parse_maturity("3month treasury") - 0.25) < 1e-9


async def test_parse_maturity_years():
    assert abs(TermStructure._parse_maturity("10year yield") - 10.0) < 1e-9


async def test_parse_maturity_abbreviated():
    assert abs(TermStructure._parse_maturity("5yr bond") - 5.0) < 1e-9


async def test_parse_maturity_no_match_returns_none():
    assert TermStructure._parse_maturity("no maturity here") is None


async def test_nss_curve_scalar_output():
    ts = TermStructure()
    params = np.array([0.04, -0.02, 0.01, 0.005, 1.5, 5.0])
    out = ts._nss_curve(np.array([1.0, 2.0, 5.0, 10.0]), params)
    assert out.shape == (4,)
    # Long-run level should approach beta0
    long_run = ts._nss_curve(np.array([100.0]), params)
    assert abs(float(long_run[0]) - params[0]) < 0.5


async def test_calibrate_vasicek_keys():
    rng = np.random.default_rng(30)
    rates = 0.04 + np.cumsum(rng.normal(0, 0.001, 50))
    result = TermStructure._calibrate_vasicek(rates, dt=1.0 / 12)
    for key in ("kappa", "theta", "sigma", "half_life_years"):
        assert key in result


async def test_calibrate_cir_feller_key():
    rng = np.random.default_rng(31)
    rates = np.abs(0.04 + np.cumsum(rng.normal(0, 0.001, 50)))
    result = TermStructure._calibrate_cir(rates, dt=1.0 / 12)
    assert "feller_condition" in result
    assert isinstance(result["feller_condition"], (bool, np.bool_))


async def test_compute_with_yield_data_returns_score(db_conn):
    """Insert a simple upward-sloping curve for multiple dates."""
    maturities_labels = [
        ("3month treasury", 0.04),
        ("2year treasury", 0.043),
        ("5yr treasury", 0.046),
        ("10year treasury", 0.048),
        ("30year treasury", 0.050),
    ]
    # Insert 30 monthly observations per tenor
    dates = [f"202{y}-{m:02d}-01" for y in range(2, 5) for m in range(1, 11)][:30]
    for label, base_yield in maturities_labels:
        sid = await _insert_series(db_conn, "treasury", label.replace(" ", "_"),
                                    "USA", label)
        rng = np.random.default_rng(hash(label) % 2 ** 31)
        vals = (base_yield + rng.normal(0, 0.001, 30)).tolist()
        await _insert_points(db_conn, sid, list(zip(dates, vals)))

    result = await TermStructure().compute(db_conn, country_iso3="USA")
    assert isinstance(result.get("score"), float)
    assert 0.0 <= result["score"] <= 100.0
