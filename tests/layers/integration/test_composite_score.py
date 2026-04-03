import pytest
from app.layers.integration.composite_score import CompositeEconomicScore
from tests.layers.integration.conftest import seed_layer_scores


def test_instantiation():
    m = CompositeEconomicScore()
    assert m.layer_id == "l6"
    assert m.name == "Composite Economic Analysis Score"


async def test_empty_db_returns_unavailable(db_conn):
    m = CompositeEconomicScore()
    result = await m.compute(db_conn, country_iso3="USA")
    assert result["signal"] == "UNAVAILABLE"
    assert result["score"] is None


async def test_weighted_average_with_seeded_scores(db_conn):
    await seed_layer_scores(db_conn, country_iso3="DEU", scores={"l1": 20.0, "l2": 20.0, "l3": 20.0, "l4": 20.0, "l5": 20.0})
    m = CompositeEconomicScore()
    result = await m.compute(db_conn, country_iso3="DEU")
    assert result["score"] == pytest.approx(20.0, abs=0.1)
    assert result["signal"] == "STABLE"


async def test_component_breakdown_keys_present(db_conn):
    await seed_layer_scores(db_conn, country_iso3="FRA")
    m = CompositeEconomicScore()
    result = await m.compute(db_conn, country_iso3="FRA")
    assert "component_breakdown" in result
    for lid in result["component_breakdown"]:
        entry = result["component_breakdown"][lid]
        assert "score" in entry and "weight" in entry and "contribution" in entry


async def test_hysteresis_retains_previous_signal_near_boundary(db_conn):
    # Score of 24.5 is just inside STABLE range; with previous_signal=WATCH,
    # hysteresis should keep WATCH because 24.5 > (25.0 - 2.0) = 23.0
    await seed_layer_scores(
        db_conn, country_iso3="JPN",
        scores={"l1": 24.5, "l2": 24.5, "l3": 24.5, "l4": 24.5, "l5": 24.5},
    )
    m = CompositeEconomicScore()
    result = await m.compute(db_conn, country_iso3="JPN", previous_signal="WATCH")
    assert result["signal"] == "WATCH"
