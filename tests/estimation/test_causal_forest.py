"""Tests for run_causal_forest."""

import numpy as np
import pandas as pd
import pytest

try:
    from econml.dml import CausalForestDML  # noqa: F401
    _HAS_ECONML = True
except ImportError:
    _HAS_ECONML = False

from app.estimation.causal_forest import run_causal_forest, CausalForestResult, variable_importance

pytestmark = pytest.mark.skipif(not _HAS_ECONML, reason="econml not installed")


@pytest.fixture()
def cf_df(rng):
    n = 300
    x1 = rng.standard_normal(n)
    x2 = rng.standard_normal(n)
    t = (rng.standard_normal(n) + 0.3 * x1 > 0).astype(float)
    y = 2.0 * t + 0.5 * x1 + rng.standard_normal(n) * 0.5
    return pd.DataFrame({"y": y, "t": t, "x1": x1, "x2": x2})


def test_run_causal_forest_returns_result(cf_df):
    result = run_causal_forest(cf_df, y="y", treatment="t", controls=["x1", "x2"], n_trees=100)
    assert isinstance(result, CausalForestResult)


def test_run_causal_forest_ate_finite(cf_df):
    result = run_causal_forest(cf_df, y="y", treatment="t", controls=["x1", "x2"], n_trees=100)
    assert np.isfinite(result.ate)


def test_run_causal_forest_n_obs(cf_df):
    result = run_causal_forest(cf_df, y="y", treatment="t", controls=["x1", "x2"], n_trees=100)
    assert result.n_obs == len(cf_df)


def test_run_causal_forest_cate_shape(cf_df):
    result = run_causal_forest(cf_df, y="y", treatment="t", controls=["x1", "x2"], n_trees=100)
    assert result.cate_predictions.shape == (len(cf_df),)


def test_variable_importance_returns_dataframe(cf_df):
    result = run_causal_forest(cf_df, y="y", treatment="t", controls=["x1", "x2"], n_trees=100)
    vi = variable_importance(result)
    assert "variable" in vi.columns
    assert "importance" in vi.columns
    assert len(vi) == 2
