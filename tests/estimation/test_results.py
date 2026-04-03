"""Tests for EstimationResult, EventStudyResult, RDDResult dataclasses."""

import numpy as np
import pytest
from app.estimation.results import EstimationResult, EventStudyResult, RDDResult, _significance_stars


def _make_result(**kwargs):
    defaults = dict(
        coef={"x": 1.0},
        se={"x": 0.5},
        pval={"x": 0.04},
        ci_lower={"x": 0.02},
        ci_upper={"x": 1.98},
        n_obs=100,
        r_sq=0.45,
    )
    defaults.update(kwargs)
    return EstimationResult(**defaults)


def test_estimation_result_instantiation():
    r = _make_result()
    assert r.n_obs == 100
    assert r.r_sq == 0.45


def test_to_dict_has_required_keys():
    r = _make_result()
    d = r.to_dict()
    for key in ("method", "depvar", "n_obs", "r_sq", "coefficients", "diagnostics"):
        assert key in d


def test_significant_at_returns_significant_vars():
    r = _make_result(pval={"x": 0.03, "z": 0.2})
    sig = r.significant_at(alpha=0.05)
    assert "x" in sig
    assert "z" not in sig


def test_repr_contains_method():
    r = _make_result(method="OLS", depvar="income")
    s = repr(r)
    assert "OLS" in s
    assert "income" in s


def test_significance_stars_levels():
    assert _significance_stars(0.0005) == "***"
    assert _significance_stars(0.005) == "**"
    assert _significance_stars(0.04) == "*"
    assert _significance_stars(0.2) == ""


def test_event_study_result_to_dict():
    es = EventStudyResult(
        periods=[-1, 0, 1],
        coef=[0.0, 1.0, 0.8],
        se=[0.1, 0.2, 0.15],
        pval=[1.0, 0.0, 0.0],
        ci_lower=[-0.2, 0.6, 0.5],
        ci_upper=[0.2, 1.4, 1.1],
    )
    d = es.to_dict()
    assert "periods" in d
    assert d["n_obs"] == 0


def test_rdd_result_has_rdd_fields():
    r = RDDResult(
        coef={"treatment_effect": 2.5},
        se={"treatment_effect": 0.3},
        pval={"treatment_effect": 0.001},
        ci_lower={"treatment_effect": 1.9},
        ci_upper={"treatment_effect": 3.1},
        n_obs=150,
        r_sq=0.3,
        bandwidth=1.0,
        n_left=75,
        n_right=75,
        cutoff=0.0,
    )
    assert r.bandwidth == 1.0
    assert r.n_left == 75
    d = r.to_dict()
    assert "bandwidth" in d
    assert "cutoff" in d
