"""Tests for run_iv and anderson_rubin_ci."""

import numpy as np
import pandas as pd
import pytest
from app.estimation.iv import run_iv, anderson_rubin_ci, AndersonRubinResult
from app.estimation.results import EstimationResult


@pytest.fixture()
def iv_df(rng):
    """IV setup: z -> d -> y with true coef = 1.5."""
    n = 150
    z = rng.standard_normal(n)
    d = 0.8 * z + rng.standard_normal(n) * 0.5
    y = 1.5 * d + rng.standard_normal(n)
    return pd.DataFrame({"y": y, "d": d, "z": z})


def test_run_iv_returns_estimation_result(iv_df):
    result = run_iv(iv_df, y="y", endog="d", instruments="z")
    assert isinstance(result, EstimationResult)


def test_run_iv_method_label(iv_df):
    result = run_iv(iv_df, y="y", endog="d", instruments="z")
    assert result.method == "IV-2SLS"


def test_run_iv_coef_close_to_true(iv_df):
    result = run_iv(iv_df, y="y", endog="d", instruments="z")
    assert abs(result.coef["d"] - 1.5) < 1.0


def test_run_iv_has_first_stage_diagnostics(iv_df):
    result = run_iv(iv_df, y="y", endog="d", instruments="z")
    assert "first_stage" in result.diagnostics
    fs = result.diagnostics["first_stage"]["d"]
    assert "f_stat" in fs
    assert fs["f_stat"] > 0


def test_run_iv_n_obs(iv_df):
    result = run_iv(iv_df, y="y", endog="d", instruments="z")
    assert result.n_obs == len(iv_df)


def test_anderson_rubin_ci_returns_result(iv_df):
    ar = anderson_rubin_ci(iv_df, y="y", endog="d", instruments=["z"], grid_points=200)
    assert isinstance(ar, AndersonRubinResult)


def test_anderson_rubin_ci_bounds_finite_or_inf(iv_df):
    ar = anderson_rubin_ci(iv_df, y="y", endog="d", instruments=["z"], grid_points=200)
    if not ar.unbounded:
        assert np.isfinite(ar.ci_lower) or np.isfinite(ar.ci_upper)


def test_anderson_rubin_ci_n_obs(iv_df):
    ar = anderson_rubin_ci(iv_df, y="y", endog="d", instruments=["z"], grid_points=200)
    assert ar.n_obs == len(iv_df)
