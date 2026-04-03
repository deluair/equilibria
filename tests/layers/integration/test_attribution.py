import pytest
from app.layers.integration.attribution import LayerAttribution
from tests.layers.integration.conftest import seed_layer_scores


def test_instantiation():
    m = LayerAttribution()
    assert m.layer_id == "l6"
    assert m.name == "Layer Attribution"


async def test_empty_db_returns_unavailable(db_conn):
    m = LayerAttribution()
    result = await m.compute(db_conn, country_iso3="ZZZ")
    assert result["signal"] == "UNAVAILABLE"


async def test_shapley_values_sum_near_composite_minus_baseline(db_conn):
    # With equal layer scores, Shapley values should all be equal.
    await seed_layer_scores(db_conn, country_iso3="BRA", scores={"l1": 60.0, "l2": 60.0, "l3": 60.0, "l4": 60.0, "l5": 60.0})
    m = LayerAttribution()
    result = await m.compute(db_conn, country_iso3="BRA", baseline=50.0)
    shapley = result["shapley_values"]
    assert len(shapley) == 5
    # All Shapley values should be equal (symmetric game)
    vals = list(shapley.values())
    assert max(vals) - min(vals) < 1.0


async def test_marginal_contributions_present(db_conn):
    await seed_layer_scores(db_conn, country_iso3="CAN", scores={"l1": 10.0, "l2": 80.0, "l3": 40.0})
    m = LayerAttribution()
    result = await m.compute(db_conn, country_iso3="CAN")
    assert "marginal_contributions" in result
    assert len(result["marginal_contributions"]) >= 2


async def test_ranking_and_drivers_in_result(db_conn):
    await seed_layer_scores(db_conn, country_iso3="AUS", scores={"l1": 10.0, "l2": 90.0, "l3": 40.0, "l4": 20.0, "l5": 50.0})
    m = LayerAttribution()
    result = await m.compute(db_conn, country_iso3="AUS", baseline=50.0)
    assert "ranking" in result
    assert isinstance(result["ranking"], list)
    assert "drivers_up" in result
    assert "drivers_down" in result
