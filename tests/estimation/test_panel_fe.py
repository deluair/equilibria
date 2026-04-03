"""Tests for run_panel_fe and hausman_test."""

import numpy as np
import pandas as pd
import pytest
from app.estimation.panel_fe import run_panel_fe, hausman_test
from app.estimation.results import EstimationResult


def test_run_panel_fe_returns_estimation_result(panel_df):
    result = run_panel_fe(panel_df, y="y", x="x", entity_col="entity", time_col="time")
    assert isinstance(result, EstimationResult)


def test_run_panel_fe_method_label(panel_df):
    result = run_panel_fe(panel_df, y="y", x="x", entity_col="entity", time_col="time")
    assert result.method == "Panel FE"


def test_run_panel_fe_coef_x_in_result(panel_df):
    result = run_panel_fe(panel_df, y="y", x="x", entity_col="entity", time_col="time")
    assert "x" in result.coef


def test_run_panel_fe_n_obs(panel_df):
    result = run_panel_fe(panel_df, y="y", x="x", entity_col="entity", time_col="time")
    assert result.n_obs == len(panel_df)


def test_run_panel_fe_diagnostics_has_within_r2(panel_df):
    result = run_panel_fe(panel_df, y="y", x="x", entity_col="entity", time_col="time")
    assert "within_r_sq" in result.diagnostics


def test_run_panel_fe_entity_only(panel_df):
    result = run_panel_fe(
        panel_df, y="y", x="x", entity_col="entity", time_col="time",
        entity_fe=True, time_fe=False,
    )
    assert isinstance(result, EstimationResult)


def test_hausman_test_returns_dict(panel_df):
    h = hausman_test(panel_df, y="y", x="x", entity_col="entity", time_col="time")
    assert isinstance(h, dict)
    assert "chi2" in h
    assert "pval" in h
    assert "prefer" in h


def test_hausman_test_prefer_valid(panel_df):
    h = hausman_test(panel_df, y="y", x="x", entity_col="entity", time_col="time")
    assert h["prefer"] in ("FE", "RE at alpha=0.05")
