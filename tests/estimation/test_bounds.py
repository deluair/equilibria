"""Tests for oster_bounds, lee_bounds, manski_bounds."""

import numpy as np
import pandas as pd
import pytest
from app.estimation.bounds import oster_bounds, lee_bounds, manski_bounds, OsterResult, LeeBoundsResult, ManskiBoundsResult
from app.estimation.results import EstimationResult


def _make_est_result(coef_val, r_sq, depvar="y", treatment="x"):
    return EstimationResult(
        coef={treatment: coef_val, "const": 0.5},
        se={treatment: 0.1, "const": 0.05},
        pval={treatment: 0.01, "const": 0.1},
        ci_lower={treatment: coef_val - 0.2, "const": 0.4},
        ci_upper={treatment: coef_val + 0.2, "const": 0.6},
        n_obs=200,
        r_sq=r_sq,
        method="OLS",
        depvar=depvar,
    )


def test_oster_bounds_returns_oster_result():
    baseline = _make_est_result(coef_val=0.5, r_sq=0.2)
    full = _make_est_result(coef_val=0.4, r_sq=0.35)
    result = oster_bounds(baseline, full, treatment="x")
    assert isinstance(result, OsterResult)


def test_oster_bounds_identified_set_ordered():
    baseline = _make_est_result(coef_val=0.5, r_sq=0.2)
    full = _make_est_result(coef_val=0.4, r_sq=0.35)
    result = oster_bounds(baseline, full, treatment="x")
    assert result.identified_set[0] <= result.identified_set[1]


def test_oster_bounds_beta_star_finite():
    baseline = _make_est_result(coef_val=0.5, r_sq=0.2)
    full = _make_est_result(coef_val=0.4, r_sq=0.35)
    result = oster_bounds(baseline, full, treatment="x")
    assert np.isfinite(result.beta_star)


def test_oster_bounds_to_dict():
    baseline = _make_est_result(coef_val=0.5, r_sq=0.2)
    full = _make_est_result(coef_val=0.4, r_sq=0.35)
    result = oster_bounds(baseline, full, treatment="x")
    d = result.to_dict()
    assert "beta_star" in d
    assert "identified_set" in d


def test_lee_bounds_returns_result(rng):
    n = 200
    treat = rng.integers(0, 2, n)
    sel = (rng.standard_normal(n) + 0.3 * treat > 0).astype(int)
    y = rng.standard_normal(n) + 0.5 * treat
    df = pd.DataFrame({"y": y, "treat": treat, "sel": sel})
    result = lee_bounds(df, y="y", treat_col="treat", selection_col="sel", n_bootstrap=100)
    assert isinstance(result, LeeBoundsResult)
    assert result.lower_bound <= result.upper_bound


def test_lee_bounds_n_obs(rng):
    n = 150
    treat = rng.integers(0, 2, n)
    sel = np.ones(n, dtype=int)
    y = rng.standard_normal(n)
    df = pd.DataFrame({"y": y, "treat": treat, "sel": sel})
    result = lee_bounds(df, y="y", treat_col="treat", selection_col="sel", n_bootstrap=50)
    assert result.n_treated + result.n_control == n


def test_manski_bounds_returns_result(rng):
    n = 200
    treat = rng.integers(0, 2, n)
    y = rng.standard_normal(n)
    df = pd.DataFrame({"y": y, "treat": treat})
    result = manski_bounds(df, y="y", treat_col="treat")
    assert isinstance(result, ManskiBoundsResult)


def test_manski_bounds_width_positive(rng):
    n = 200
    treat = rng.integers(0, 2, n)
    y = rng.standard_normal(n)
    df = pd.DataFrame({"y": y, "treat": treat})
    result = manski_bounds(df, y="y", treat_col="treat")
    assert result.upper_bound >= result.lower_bound
