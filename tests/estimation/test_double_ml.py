"""Tests for run_double_ml."""

import numpy as np
import pandas as pd
import pytest

try:
    import doubleml  # noqa: F401
    _HAS_DML = True
except ImportError:
    _HAS_DML = False

from app.estimation.double_ml import run_double_ml
from app.estimation.results import EstimationResult

pytestmark = pytest.mark.skipif(not _HAS_DML, reason="doubleml not installed")


@pytest.fixture()
def dml_df(rng):
    n = 200
    x1 = rng.standard_normal(n)
    x2 = rng.standard_normal(n)
    d = 0.5 * x1 + 0.3 * x2 + rng.standard_normal(n) * 0.5
    y = 1.0 * d + 0.4 * x1 + rng.standard_normal(n) * 0.5
    return pd.DataFrame({"y": y, "d": d, "x1": x1, "x2": x2})


def test_run_double_ml_returns_estimation_result(dml_df):
    result = run_double_ml(dml_df, y="y", treatment="d", controls=["x1", "x2"], ml_method="lasso", n_folds=3)
    assert isinstance(result, EstimationResult)


def test_run_double_ml_has_treatment_coef(dml_df):
    result = run_double_ml(dml_df, y="y", treatment="d", controls=["x1", "x2"], ml_method="lasso", n_folds=3)
    assert "d" in result.coef


def test_run_double_ml_method_label(dml_df):
    result = run_double_ml(dml_df, y="y", treatment="d", controls=["x1", "x2"], ml_method="lasso", n_folds=3)
    assert "DML" in result.method


def test_run_double_ml_n_obs(dml_df):
    result = run_double_ml(dml_df, y="y", treatment="d", controls=["x1", "x2"], ml_method="lasso", n_folds=3)
    assert result.n_obs == len(dml_df)
