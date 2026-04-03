import pytest
from app.layers.integration.crisis_comparison import CrisisComparison, CRISIS_PROFILES
from tests.layers.integration.conftest import seed_layer_scores


def test_instantiation():
    m = CrisisComparison()
    assert m.layer_id == "l6"
    assert m.name == "Crisis Comparison"


async def test_insufficient_layers_returns_unavailable(db_conn):
    # Only seed 2 layers (need >= 3)
    await seed_layer_scores(db_conn, country_iso3="ZZZ", scores={"l1": 40.0, "l2": 50.0})
    m = CrisisComparison()
    result = await m.compute(db_conn, country_iso3="ZZZ")
    assert result["signal"] == "UNAVAILABLE"


async def test_result_includes_most_similar_crisis(db_conn):
    await seed_layer_scores(db_conn, country_iso3="USA")
    m = CrisisComparison()
    result = await m.compute(db_conn, country_iso3="USA")
    assert "most_similar_crisis" in result
    assert result["most_similar_crisis"]["id"] in CRISIS_PROFILES


async def test_distances_contain_all_crisis_profiles(db_conn):
    await seed_layer_scores(db_conn, country_iso3="DEU")
    m = CrisisComparison()
    result = await m.compute(db_conn, country_iso3="DEU")
    for cid in CRISIS_PROFILES:
        assert cid in result["distances"]


async def test_score_is_bounded(db_conn):
    await seed_layer_scores(db_conn, country_iso3="JPN")
    m = CrisisComparison()
    result = await m.compute(db_conn, country_iso3="JPN")
    assert 0.0 <= result["score"] <= 100.0
