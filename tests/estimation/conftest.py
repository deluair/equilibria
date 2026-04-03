"""Shared fixtures for estimation module tests."""

import numpy as np
import pandas as pd
import pytest


@pytest.fixture()
def rng():
    return np.random.default_rng(42)


@pytest.fixture()
def ols_df(rng):
    """Small DataFrame suitable for OLS: y = 2*x + noise."""
    n = 80
    x = rng.standard_normal(n)
    y = 2.0 * x + rng.standard_normal(n)
    return pd.DataFrame({"y": y, "x": x})


@pytest.fixture()
def panel_df(rng):
    """Small balanced panel: 10 entities x 8 periods."""
    n_entity, n_time = 10, 8
    entities = np.repeat(np.arange(n_entity), n_time)
    times = np.tile(np.arange(n_time), n_entity)
    x = rng.standard_normal(n_entity * n_time)
    fe_entity = np.repeat(rng.standard_normal(n_entity), n_time)
    y = 1.5 * x + fe_entity + rng.standard_normal(n_entity * n_time) * 0.5
    return pd.DataFrame({"entity": entities, "time": times, "y": y, "x": x})


@pytest.fixture()
def did_df(rng):
    """2x2 DID DataFrame: 100 obs, clear positive treatment effect."""
    n = 100
    treat = rng.integers(0, 2, n)
    post = rng.integers(0, 2, n)
    y = 1.0 + 0.5 * treat + 0.3 * post + 2.0 * treat * post + rng.standard_normal(n) * 0.5
    return pd.DataFrame({"y": y, "treat": treat, "post": post})
