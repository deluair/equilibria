"""Tests for run_shift_share, _construct_bartik, run_adh_balance."""

import numpy as np
import pandas as pd
import pytest
from app.estimation.shift_share import run_shift_share, _construct_bartik, run_adh_balance, ShiftShareResult


@pytest.fixture()
def ss_data(rng):
    """Shift-share setup: 20 regions, 5 sectors."""
    n_regions, n_sectors = 20, 5
    regions = [f"r{i}" for i in range(n_regions)]
    sectors = [f"s{k}" for k in range(n_sectors)]

    # Random shares (normalized to sum to 1 per region)
    raw = np.abs(rng.standard_normal((n_regions, n_sectors))) + 0.1
    shares_arr = raw / raw.sum(axis=1, keepdims=True)
    shares = pd.DataFrame(shares_arr, index=regions, columns=sectors)

    # Random sector shifts
    shifts = pd.Series(rng.standard_normal(n_sectors), index=sectors)

    # Outcome correlated with bartik instrument
    bartik_vals = shares_arr @ shifts.values
    y_vals = 0.8 * bartik_vals + rng.standard_normal(n_regions) * 0.3
    df = pd.DataFrame({"y": y_vals}, index=regions)

    return df, shares, shifts


def test_construct_bartik_returns_series(ss_data):
    df, shares, shifts = ss_data
    bartik = _construct_bartik(shares, shifts)
    assert isinstance(bartik, pd.Series)
    assert len(bartik) == len(shares)


def test_run_shift_share_returns_result(ss_data):
    df, shares, shifts = ss_data
    result = run_shift_share(df, y="y", shares=shares, shifts=shifts)
    assert isinstance(result, ShiftShareResult)


def test_run_shift_share_n_obs(ss_data):
    df, shares, shifts = ss_data
    result = run_shift_share(df, y="y", shares=shares, shifts=shifts)
    assert result.n_obs == len(df)


def test_run_shift_share_n_sectors(ss_data):
    df, shares, shifts = ss_data
    result = run_shift_share(df, y="y", shares=shares, shifts=shifts)
    assert result.n_sectors == 5


def test_run_shift_share_rotemberg_weights_dict(ss_data):
    df, shares, shifts = ss_data
    result = run_shift_share(df, y="y", shares=shares, shifts=shifts)
    assert isinstance(result.rotemberg_weights, dict)
    assert len(result.rotemberg_weights) == 5


def test_run_adh_balance_returns_dataframe(ss_data):
    df, shares, shifts = ss_data
    controls = pd.DataFrame({"pop": np.random.randn(len(df))}, index=df.index)
    out = run_adh_balance(shares, shifts, controls)
    assert isinstance(out, pd.DataFrame)
    assert "coef" in out.columns
    assert "pval" in out.columns


def test_run_shift_share_with_endogenous(ss_data, rng):
    df, shares, shifts = ss_data
    bartik = _construct_bartik(shares, shifts)
    df = df.copy()
    df["d"] = bartik.values * 0.7 + rng.standard_normal(len(df)) * 0.2
    result = run_shift_share(df, y="y", shares=shares, shifts=shifts, endogenous="d")
    assert isinstance(result, ShiftShareResult)
    assert np.isfinite(result.iv_coef)
