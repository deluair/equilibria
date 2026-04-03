import numpy as np
import pytest
from app.layers.public.public_goods import PublicGoods, _ramsey_discount_rate, _turnbull_wtp


# --- Unit tests for pure functions ---

def test_ramsey_basic():
    r = _ramsey_discount_rate(0.01, 1.5, 0.02)
    assert r == pytest.approx(0.01 + 1.5 * 0.02)


def test_ramsey_zero_growth():
    r = _ramsey_discount_rate(0.02, 1.5, 0.0)
    assert r == pytest.approx(0.02)


def test_turnbull_wtp_monotone_input():
    bids = np.array([10.0, 20.0, 30.0])
    rates = np.array([0.8, 0.5, 0.2])
    wtp = _turnbull_wtp(bids, rates)
    assert wtp > 0


def test_turnbull_wtp_non_monotone_enforced():
    # Input rates are non-monotone; function should enforce monotonicity
    bids = np.array([10.0, 20.0, 30.0])
    rates = np.array([0.5, 0.7, 0.2])  # second rate higher than first
    wtp = _turnbull_wtp(bids, rates)
    assert isinstance(wtp, float)
    assert wtp >= 0


# --- Integration tests with empty DB ---

async def test_instantiation():
    model = PublicGoods()
    assert model is not None


async def test_layer_id():
    model = PublicGoods()
    assert model.layer_id == "l10"


async def test_name():
    model = PublicGoods()
    assert model.name == "Public Goods Provision"


async def test_compute_returns_dict(db_conn):
    model = PublicGoods()
    result = await model.compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)


async def test_compute_has_score(db_conn):
    model = PublicGoods()
    result = await model.compute(db_conn, country_iso3="USA")
    assert "score" in result
    assert isinstance(result["score"], (int, float))


async def test_compute_score_in_range(db_conn):
    model = PublicGoods()
    result = await model.compute(db_conn, country_iso3="USA")
    assert 0.0 <= result["score"] <= 100.0


async def test_compute_has_samuelson_key(db_conn):
    model = PublicGoods()
    result = await model.compute(db_conn, country_iso3="USA")
    assert "samuelson" in result["results"]


async def test_compute_has_benefit_cost_key(db_conn):
    model = PublicGoods()
    result = await model.compute(db_conn, country_iso3="USA")
    assert "benefit_cost" in result["results"]
    bca = result["results"]["benefit_cost"]
    assert "ramsey_discount_rate" in bca
    assert bca["ramsey_discount_rate"] > 0


async def test_compute_project_evaluation_when_provided(db_conn):
    model = PublicGoods()
    result = await model.compute(
        db_conn,
        country_iso3="USA",
        project_cost=1_000_000,
        project_annual_benefit=80_000,
        project_years=20,
    )
    bca = result["results"]["benefit_cost"]
    assert "project_evaluation" in bca
    pe = bca["project_evaluation"]
    assert "npv" in pe
    assert "bcr" in pe
    assert pe["recommendation"] in ("accept", "reject")


async def test_compute_contingent_valuation_no_data(db_conn):
    model = PublicGoods()
    result = await model.compute(db_conn, country_iso3="USA")
    cv = result["results"]["contingent_valuation"]
    assert "error" in cv


async def test_compute_custom_discount_params(db_conn):
    model = PublicGoods()
    result = await model.compute(
        db_conn,
        country_iso3="USA",
        pure_time_preference=0.02,
        utility_elasticity=2.0,
    )
    bca = result["results"]["benefit_cost"]
    assert bca["components"]["pure_time_preference"] == 0.02
    assert bca["components"]["utility_elasticity"] == 2.0


async def test_run_adds_signal(db_conn):
    model = PublicGoods()
    result = await model.run(db_conn, country_iso3="USA")
    assert "signal" in result
    assert result["signal"] in ("STABLE", "WATCH", "STRESS", "CRISIS", "UNAVAILABLE")
