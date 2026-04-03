import pytest
from app.layers.public.infrastructure import InfrastructureEconomics, _bpr_travel_time, _marginal_congestion_cost


# --- Unit tests for pure functions ---

def test_bpr_free_flow_at_zero_volume():
    t = _bpr_travel_time(0.0, 2000.0, 20.0)
    assert t == pytest.approx(20.0)


def test_bpr_increases_with_volume():
    t_low = _bpr_travel_time(500.0, 2000.0, 20.0)
    t_high = _bpr_travel_time(1800.0, 2000.0, 20.0)
    assert t_high > t_low


def test_bpr_at_capacity():
    t = _bpr_travel_time(2000.0, 2000.0, 20.0)
    # alpha=0.15 -> t = 20 * (1 + 0.15) = 23.0
    assert t == pytest.approx(20.0 * 1.15, rel=0.01)


def test_marginal_congestion_cost_zero_volume():
    mcc = _marginal_congestion_cost(0.0, 2000.0, 20.0, 25.0)
    assert mcc == pytest.approx(0.0)


def test_marginal_congestion_cost_positive_at_high_volume():
    mcc = _marginal_congestion_cost(1800.0, 2000.0, 20.0, 25.0)
    assert mcc > 0.0


# --- Integration tests with empty DB ---

async def test_instantiation():
    model = InfrastructureEconomics()
    assert model is not None


async def test_layer_id():
    model = InfrastructureEconomics()
    assert model.layer_id == "l10"


async def test_name():
    model = InfrastructureEconomics()
    assert model.name == "Infrastructure Economics"


async def test_compute_returns_dict(db_conn):
    model = InfrastructureEconomics()
    result = await model.compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)


async def test_compute_has_score(db_conn):
    model = InfrastructureEconomics()
    result = await model.compute(db_conn, country_iso3="USA")
    assert "score" in result
    assert isinstance(result["score"], (int, float))


async def test_compute_score_in_range(db_conn):
    model = InfrastructureEconomics()
    result = await model.compute(db_conn, country_iso3="USA")
    assert 0.0 <= result["score"] <= 100.0


async def test_compute_has_aschauer_key(db_conn):
    model = InfrastructureEconomics()
    result = await model.compute(db_conn, country_iso3="USA")
    assert "aschauer" in result["results"]


async def test_compute_has_infrastructure_gap_key(db_conn):
    model = InfrastructureEconomics()
    result = await model.compute(db_conn, country_iso3="USA")
    assert "infrastructure_gap" in result["results"]


async def test_compute_congestion_always_present(db_conn):
    model = InfrastructureEconomics()
    result = await model.compute(db_conn, country_iso3="USA")
    cong = result["results"]["congestion_pricing"]
    assert "optimal_toll" in cong
    assert "volume_capacity_ratio" in cong
    assert "congestion_level" in cong


async def test_compute_congestion_level_classification(db_conn):
    model = InfrastructureEconomics()
    result = await model.compute(
        db_conn,
        country_iso3="USA",
        current_volume_vehicles_hr=2200.0,
        road_capacity_vehicles_hr=2000.0,
    )
    cong = result["results"]["congestion_pricing"]
    assert cong["congestion_level"] == "severe"


async def test_compute_ppp_evaluation_with_cost(db_conn):
    model = InfrastructureEconomics()
    result = await model.compute(
        db_conn,
        country_iso3="USA",
        project_cost=10_000_000,
        project_years=20,
    )
    ppp = result["results"]["ppp_evaluation"]
    assert "value_for_money" in ppp
    assert "recommendation" in ppp
    assert ppp["recommendation"] in ("PPP preferred", "public procurement preferred")


async def test_run_adds_signal(db_conn):
    model = InfrastructureEconomics()
    result = await model.run(db_conn, country_iso3="USA")
    assert "signal" in result
    assert result["signal"] in ("STABLE", "WATCH", "STRESS", "CRISIS", "UNAVAILABLE")
