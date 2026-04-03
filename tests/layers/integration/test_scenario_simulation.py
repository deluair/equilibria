import pytest
from app.layers.integration.scenario_simulation import ScenarioSimulation, PREDEFINED_SHOCKS
from tests.layers.integration.conftest import seed_layer_scores


def test_instantiation():
    m = ScenarioSimulation()
    assert m.layer_id == "l6"
    assert m.name == "Scenario Simulation"


async def test_unknown_scenario_returns_error(db_conn):
    m = ScenarioSimulation()
    result = await m.compute(db_conn, country_iso3="USA", scenario_id="nonexistent_shock")
    assert result["signal"] == "UNAVAILABLE"
    assert "available_scenarios" in result


async def test_predefined_scenario_raises_scores(db_conn):
    # Seed moderate scores and apply a tariff war shock
    await seed_layer_scores(db_conn, country_iso3="USA", scores={"l1": 30.0, "l2": 30.0, "l3": 30.0, "l4": 30.0, "l5": 30.0})
    m = ScenarioSimulation()
    result = await m.compute(db_conn, country_iso3="USA", scenario_id="tariff_war")
    assert result["post_shock"]["composite"] > result["pre_shock"]["composite"]
    assert result["impact"]["composite_change"] > 0.0


async def test_custom_shock_accepted(db_conn):
    await seed_layer_scores(db_conn, country_iso3="BRA", scores={"l1": 20.0, "l2": 20.0, "l3": 20.0})
    m = ScenarioSimulation()
    result = await m.compute(
        db_conn,
        country_iso3="BRA",
        custom_shock={"l1": 10.0, "l2": 5.0, "l3": 3.0},
        description="Custom test shock",
    )
    assert result["scenario"]["name"] == "Custom Shock"
    assert "post_shock" in result


async def test_post_shock_scores_bounded(db_conn):
    await seed_layer_scores(db_conn, country_iso3="CAN", scores={"l1": 90.0, "l2": 90.0, "l3": 90.0, "l4": 90.0, "l5": 90.0})
    m = ScenarioSimulation()
    result = await m.compute(db_conn, country_iso3="CAN", scenario_id="pandemic_shock", magnitude=2.0)
    for lid, score in result["post_shock"]["layer_scores"].items():
        assert 0.0 <= score <= 100.0
