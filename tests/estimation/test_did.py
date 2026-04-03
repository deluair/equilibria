"""Tests for run_did and run_event_study."""

import numpy as np
import pandas as pd
import pytest
from app.estimation.did import run_did, run_event_study
from app.estimation.results import EstimationResult, EventStudyResult


def test_run_did_returns_estimation_result(did_df):
    result = run_did(did_df, y="y", treat_col="treat", post_col="post")
    assert isinstance(result, EstimationResult)


def test_run_did_method_label(did_df):
    result = run_did(did_df, y="y", treat_col="treat", post_col="post")
    assert result.method == "DID"


def test_run_did_interaction_coef_positive(did_df):
    result = run_did(did_df, y="y", treat_col="treat", post_col="post")
    assert result.coef["treat_x_post"] > 0


def test_run_did_n_obs(did_df):
    result = run_did(did_df, y="y", treat_col="treat", post_col="post")
    assert result.n_obs == len(did_df)


def test_run_did_diagnostics_has_did_estimate(did_df):
    result = run_did(did_df, y="y", treat_col="treat", post_col="post")
    assert "did_estimate" in result.diagnostics


def test_run_event_study_returns_event_study_result(rng):
    n = 200
    entity = np.repeat(np.arange(20), 10)
    t = np.tile(np.arange(10), 20)
    treat = np.repeat((np.arange(20) < 10).astype(int), 10)
    event_time = t - 5
    y = 0.5 * treat * (event_time >= 0) + rng.standard_normal(n) * 0.3
    df = pd.DataFrame({"y": y, "treat": treat, "t": t, "event_time": event_time})
    result = run_event_study(df, y="y", treat_col="treat", time_col="t", event_time_col="event_time")
    assert isinstance(result, EventStudyResult)


def test_run_event_study_has_periods(rng):
    n = 200
    entity = np.repeat(np.arange(20), 10)
    t = np.tile(np.arange(10), 20)
    treat = np.repeat((np.arange(20) < 10).astype(int), 10)
    event_time = t - 5
    y = rng.standard_normal(n)
    df = pd.DataFrame({"y": y, "treat": treat, "t": t, "event_time": event_time})
    result = run_event_study(df, y="y", treat_col="treat", time_col="t", event_time_col="event_time")
    assert len(result.periods) > 0


def test_run_event_study_coef_length_matches_periods(rng):
    n = 200
    t = np.tile(np.arange(10), 20)
    treat = np.repeat((np.arange(20) < 10).astype(int), 10)
    event_time = t - 5
    y = rng.standard_normal(n)
    df = pd.DataFrame({"y": y, "treat": treat, "t": t, "event_time": event_time})
    result = run_event_study(df, y="y", treat_col="treat", time_col="t", event_time_col="event_time")
    assert len(result.coef) == len(result.periods)
