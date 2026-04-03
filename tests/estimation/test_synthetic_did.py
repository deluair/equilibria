"""Tests for run_sdid."""

import numpy as np
import pandas as pd
import pytest
from app.estimation.synthetic_did import run_sdid, SDIDResult


@pytest.fixture()
def sdid_df(rng):
    """Balanced panel: 8 control + 2 treated units, 6 pre + 2 post periods."""
    n_control, n_treated = 8, 2
    n_pre, n_post = 6, 2
    n_units = n_control + n_treated
    n_time = n_pre + n_post

    entities = [f"u{i}" for i in range(n_units)]
    times = list(range(n_time))
    records = []
    for i, e in enumerate(entities):
        is_treated = i >= n_control
        for t in times:
            is_post = t >= n_pre
            y = (1.0 + rng.standard_normal() * 0.3
                 + (2.0 if (is_treated and is_post) else 0.0))
            records.append({
                "entity": e,
                "time": t,
                "y": y,
                "treat": int(is_treated),
                "post": int(is_post),
            })
    return pd.DataFrame(records)


def test_run_sdid_returns_sdid_result(sdid_df):
    result = run_sdid(sdid_df, y="y", entity_col="entity", time_col="time",
                      treat_col="treat", post_col="post", n_placebo=20)
    assert isinstance(result, SDIDResult)


def test_run_sdid_att_finite(sdid_df):
    result = run_sdid(sdid_df, y="y", entity_col="entity", time_col="time",
                      treat_col="treat", post_col="post", n_placebo=20)
    assert np.isfinite(result.att)


def test_run_sdid_unit_weights_sum_to_one(sdid_df):
    result = run_sdid(sdid_df, y="y", entity_col="entity", time_col="time",
                      treat_col="treat", post_col="post", n_placebo=20)
    total_w = sum(result.unit_weights.values())
    assert abs(total_w - 1.0) < 1e-3


def test_run_sdid_counts(sdid_df):
    result = run_sdid(sdid_df, y="y", entity_col="entity", time_col="time",
                      treat_col="treat", post_col="post", n_placebo=20)
    assert result.n_treated == 2
    assert result.n_control == 8
    assert result.n_pre == 6
    assert result.n_post == 2


def test_run_sdid_to_dict(sdid_df):
    result = run_sdid(sdid_df, y="y", entity_col="entity", time_col="time",
                      treat_col="treat", post_col="post", n_placebo=20)
    d = result.to_dict()
    assert "att" in d
    assert "unit_weights" in d


def test_run_sdid_invalid_raises(sdid_df):
    bad_df = sdid_df.copy()
    bad_df["treat"] = 0
    with pytest.raises(ValueError):
        run_sdid(bad_df, y="y", entity_col="entity", time_col="time",
                 treat_col="treat", post_col="post", n_placebo=5)
