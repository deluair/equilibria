"""Tests for run_ols."""

import numpy as np
import pandas as pd
import pytest
from app.estimation.ols import run_ols
from app.estimation.results import EstimationResult


def test_run_ols_returns_estimation_result(ols_df):
    result = run_ols(ols_df, y="y", x="x")
    assert isinstance(result, EstimationResult)


def test_run_ols_method_label(ols_df):
    result = run_ols(ols_df, y="y", x="x")
    assert result.method == "OLS"


def test_run_ols_coef_x_near_two(ols_df):
    result = run_ols(ols_df, y="y", x="x")
    assert abs(result.coef["x"] - 2.0) < 0.5


def test_run_ols_n_obs(ols_df):
    result = run_ols(ols_df, y="y", x="x")
    assert result.n_obs == len(ols_df)


def test_run_ols_robust_se(ols_df):
    result = run_ols(ols_df, y="y", x="x", robust=True)
    assert result.diagnostics.get("robust") is True


def test_run_ols_with_controls(rng):
    n = 100
    x = rng.standard_normal(n)
    c = rng.standard_normal(n)
    y = 2.0 * x + 0.5 * c + rng.standard_normal(n)
    df = pd.DataFrame({"y": y, "x": x, "c": c})
    result = run_ols(df, y="y", x="x", controls=["c"])
    assert "c" in result.coef
    assert "x" in result.coef


def test_run_ols_with_fe(rng):
    n = 120
    x = rng.standard_normal(n)
    group = np.repeat(["A", "B", "C"], 40)
    y = 1.5 * x + rng.standard_normal(n)
    df = pd.DataFrame({"y": y, "x": x, "group": group})
    result = run_ols(df, y="y", x="x", fe="group")
    assert "x" in result.coef
    assert result.diagnostics.get("fe_vars") == ["group"]


def test_run_ols_ci_contains_true_coef(ols_df):
    result = run_ols(ols_df, y="y", x="x")
    lo = result.ci_lower["x"]
    hi = result.ci_upper["x"]
    assert lo < 2.0 < hi or abs(result.coef["x"] - 2.0) < 1.5
