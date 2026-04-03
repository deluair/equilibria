"""Tests for randomization_test."""

import numpy as np
import pandas as pd
import pytest
from app.estimation.randomization_inference import randomization_test, RandInfResult


@pytest.fixture()
def ri_df(rng):
    n = 60
    treat = np.zeros(n, dtype=int)
    treat[:30] = 1
    rng.shuffle(treat)
    y = 2.0 * treat + rng.standard_normal(n) * 0.5
    return pd.DataFrame({"y": y, "treat": treat})


def test_randomization_test_returns_result(ri_df):
    result = randomization_test(ri_df, y="y", treat_col="treat", n_permutations=500)
    assert isinstance(result, RandInfResult)


def test_randomization_test_pvalue_in_unit_interval(ri_df):
    result = randomization_test(ri_df, y="y", treat_col="treat", n_permutations=500)
    assert 0.0 <= result.pvalue <= 1.0


def test_randomization_test_null_dist_shape(ri_df):
    result = randomization_test(ri_df, y="y", treat_col="treat", n_permutations=300)
    assert result.null_distribution.shape == (300,)


def test_randomization_test_observed_stat_positive(ri_df):
    result = randomization_test(ri_df, y="y", treat_col="treat", n_permutations=500)
    assert result.observed_stat > 0


def test_randomization_test_n_obs(ri_df):
    result = randomization_test(ri_df, y="y", treat_col="treat", n_permutations=200)
    assert result.n_obs == len(ri_df)


def test_randomization_test_statistic_ks(ri_df):
    result = randomization_test(ri_df, y="y", treat_col="treat", statistic="ks", n_permutations=200)
    assert result.statistic_name == "ks"
    assert 0.0 <= result.pvalue <= 1.0


def test_randomization_test_statistic_rank_sum(ri_df):
    result = randomization_test(ri_df, y="y", treat_col="treat", statistic="rank_sum", n_permutations=200)
    assert result.statistic_name == "rank_sum"


def test_randomization_test_to_dict(ri_df):
    result = randomization_test(ri_df, y="y", treat_col="treat", n_permutations=100)
    d = result.to_dict()
    assert "observed_stat" in d
    assert "pvalue" in d
    assert "n_permutations" in d
