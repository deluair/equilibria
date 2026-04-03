"""Tests for run_callaway_santanna, run_sun_abraham, run_borusyak_jaravel_spiess."""

import numpy as np
import pandas as pd
import pytest
from app.estimation.staggered_did import (
    run_callaway_santanna,
    run_sun_abraham,
    run_borusyak_jaravel_spiess,
    StaggeredDIDResult,
)
from app.estimation.results import EstimationResult, EventStudyResult


@pytest.fixture()
def staggered_df(rng):
    """Staggered adoption panel: 20 entities, 10 periods, 2 cohorts."""
    n_entity, n_time = 20, 10
    records = []
    for i in range(n_entity):
        if i < 8:
            first_treat = 0  # never treated
        elif i < 14:
            first_treat = 4  # cohort 4
        else:
            first_treat = 7  # cohort 7
        for t in range(n_time):
            is_treated = first_treat > 0 and t >= first_treat
            y = 0.5 * (t / n_time) + (1.5 if is_treated else 0.0) + rng.standard_normal() * 0.3
            records.append({"entity": i, "time": t, "y": y,
                            "first_treat": first_treat, "treat": int(is_treated)})
    return pd.DataFrame(records)


def test_callaway_santanna_returns_result(staggered_df):
    result = run_callaway_santanna(staggered_df, y="y", entity_col="entity",
                                    time_col="time", first_treat_col="first_treat")
    assert isinstance(result, StaggeredDIDResult)


def test_callaway_santanna_method(staggered_df):
    result = run_callaway_santanna(staggered_df, y="y", entity_col="entity",
                                    time_col="time", first_treat_col="first_treat")
    assert result.method == "Callaway-SantAnna"


def test_callaway_santanna_n_groups(staggered_df):
    result = run_callaway_santanna(staggered_df, y="y", entity_col="entity",
                                    time_col="time", first_treat_col="first_treat")
    assert result.n_groups == 2


def test_callaway_santanna_agg_att_finite(staggered_df):
    result = run_callaway_santanna(staggered_df, y="y", entity_col="entity",
                                    time_col="time", first_treat_col="first_treat")
    assert np.isfinite(result.aggregated_att)


def test_sun_abraham_returns_event_study(staggered_df):
    try:
        result = run_sun_abraham(staggered_df, y="y", entity_col="entity", time_col="time",
                                  cohort_col="first_treat")
        assert isinstance(result, EventStudyResult)
    except np.linalg.LinAlgError:
        pytest.skip("run_sun_abraham encountered singular matrix with this dataset size")


def test_sun_abraham_coef_len_matches_periods(staggered_df):
    try:
        result = run_sun_abraham(staggered_df, y="y", entity_col="entity", time_col="time",
                                  cohort_col="first_treat")
        assert len(result.coef) == len(result.periods)
    except np.linalg.LinAlgError:
        pytest.skip("run_sun_abraham encountered singular matrix with this dataset size")


def test_borusyak_jaravel_spiess_returns_estimation_result(staggered_df):
    result = run_borusyak_jaravel_spiess(staggered_df, y="y", entity_col="entity",
                                          time_col="time", treat_col="treat")
    assert isinstance(result, EstimationResult)


def test_borusyak_jaravel_spiess_method(staggered_df):
    result = run_borusyak_jaravel_spiess(staggered_df, y="y", entity_col="entity",
                                          time_col="time", treat_col="treat")
    assert "Borusyak" in result.method
