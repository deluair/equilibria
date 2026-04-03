import pytest
from app.layers.public.social_protection import SocialProtection


async def test_instantiation():
    model = SocialProtection()
    assert model is not None


async def test_layer_id():
    model = SocialProtection()
    assert model.layer_id == "l10"


async def test_name():
    model = SocialProtection()
    assert model.name == "Social Protection"


async def test_compute_returns_dict(db_conn):
    model = SocialProtection()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)


async def test_compute_has_score(db_conn):
    model = SocialProtection()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert "score" in result
    assert isinstance(result["score"], (int, float))


async def test_compute_score_in_range(db_conn):
    model = SocialProtection()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert 0.0 <= result["score"] <= 100.0


async def test_compute_has_targeting_key(db_conn):
    model = SocialProtection()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert "targeting" in result["results"]


async def test_compute_has_poverty_trap_key(db_conn):
    model = SocialProtection()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert "poverty_trap" in result["results"]
    pt = result["results"]["poverty_trap"]
    assert "effective_marginal_tax_rate" in pt
    assert "trap_risk" in pt


async def test_compute_emtr_calculation(db_conn):
    """EMTR = withdrawal_rate + marginal_tax_rate."""
    model = SocialProtection()
    result = await model.compute(
        db_conn,
        country_iso3="BGD",
        benefit_withdrawal_rate=0.6,
        marginal_tax_rate=0.2,
    )
    pt = result["results"]["poverty_trap"]
    assert pt["effective_marginal_tax_rate"] == pytest.approx(0.8, abs=0.001)
    assert pt["trap_risk"] == "high"


async def test_compute_targeting_from_kwargs(db_conn):
    """Targeting metrics computed from direct kwargs."""
    model = SocialProtection()
    result = await model.compute(
        db_conn,
        country_iso3="BGD",
        coverage_poor=0.70,
        coverage_nonpoor=0.10,
    )
    tgt = result["results"]["targeting"]
    assert "coverage_poor" in tgt
    assert tgt["coverage_poor"] == pytest.approx(0.70, abs=0.001)
    assert tgt["exclusion_error"] == pytest.approx(0.30, abs=0.001)
    assert tgt["quality"] == "well-targeted"


async def test_compute_ubi_cost_no_data(db_conn):
    model = SocialProtection()
    result = await model.compute(db_conn, country_iso3="BGD")
    # No population/GDP data in empty DB
    ubi = result["results"]["ubi_cost"]
    assert "error" in ubi


async def test_run_adds_signal(db_conn):
    model = SocialProtection()
    result = await model.run(db_conn, country_iso3="BGD")
    assert "signal" in result
    assert result["signal"] in ("STABLE", "WATCH", "STRESS", "CRISIS", "UNAVAILABLE")
