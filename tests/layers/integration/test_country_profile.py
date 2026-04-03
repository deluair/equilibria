import pytest
from app.layers.integration.country_profile import CountryProfile, STRENGTH_THRESHOLD, VULNERABILITY_THRESHOLD
from tests.layers.integration.conftest import seed_layer_scores


def test_instantiation():
    m = CountryProfile()
    assert m.layer_id == "l6"
    assert m.name == "Country Profile"


async def test_no_scores_returns_unavailable(db_conn):
    m = CountryProfile()
    result = await m.compute(db_conn, country_iso3="ZZZ")
    assert result["signal"] == "UNAVAILABLE"


async def test_strengths_identified_for_low_scores(db_conn):
    # All layers below STRENGTH_THRESHOLD
    await seed_layer_scores(db_conn, country_iso3="DEU", scores={"l1": 10.0, "l2": 15.0, "l3": 20.0, "l4": 12.0, "l5": 8.0})
    m = CountryProfile()
    result = await m.compute(db_conn, country_iso3="DEU")
    assert len(result["strengths"]) == 5
    assert result["vulnerabilities"] == []


async def test_vulnerabilities_identified_for_high_scores(db_conn):
    # All layers above VULNERABILITY_THRESHOLD
    await seed_layer_scores(db_conn, country_iso3="ZWE", scores={"l1": 70.0, "l2": 80.0, "l3": 75.0, "l4": 65.0, "l5": 90.0})
    m = CountryProfile()
    result = await m.compute(db_conn, country_iso3="ZWE")
    assert len(result["vulnerabilities"]) == 5
    assert result["strengths"] == []


async def test_risk_assessment_keys_present(db_conn):
    await seed_layer_scores(db_conn, country_iso3="IND")
    m = CountryProfile()
    result = await m.compute(db_conn, country_iso3="IND")
    ra = result["risk_assessment"]
    for key in ("risk_level", "outlook", "n_strengths", "n_vulnerabilities"):
        assert key in ra
