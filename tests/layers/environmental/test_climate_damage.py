import numpy as np
import pytest
from app.layers.environmental.climate_damage import ClimateDamage


def test_instantiation():
    assert ClimateDamage() is not None


def test_layer_id():
    assert ClimateDamage.layer_id == "l9"


def test_name():
    assert ClimateDamage().name == "Climate Damage"


async def test_compute_empty_db_unavailable(db_conn):
    result = await ClimateDamage().compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)
    assert result.get("signal") == "UNAVAILABLE" or result.get("score") is None


async def test_compute_with_gdp_data(db_conn):
    for sid, sname in [
        ("NY.GDP.PCAP.KD", "GDP pc"),
        ("NY.GDP.MKTP.KD", "GDP"),
    ]:
        await db_conn.execute(
            "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?,?,?,?)",
            ("wdi", sid, "BGD", sname),
        )
    for sid, base_val in [("NY.GDP.PCAP.KD", 2000.0), ("NY.GDP.MKTP.KD", 3e11)]:
        row_id = (await db_conn.fetch_one(
            "SELECT id FROM data_series WHERE series_id=? AND country_iso3='BGD'", (sid,)
        ))["id"]
        for i in range(6):
            yr = 2015 + i
            await db_conn.execute(
                "INSERT INTO data_points (series_id, date, value) VALUES (?,?,?)",
                (row_id, f"{yr}-01-01", base_val * (1 + i * 0.03)),
            )

    result = await ClimateDamage().compute(db_conn, country_iso3="BGD", baseline_temp_c=26.0)
    assert isinstance(result, dict)
    assert "score" in result
    if result["score"] is not None:
        assert 0 <= result["score"] <= 100


def test_bhm_damage_above_optimal_returns_expected_keys():
    m = ClimateDamage()
    dmg = m._bhm_damage(baseline_temp=26.0, gdp_pc=2000, gdp_total=3e11)
    assert dmg["above_optimal"] is True
    assert "damages_by_scenario" in dmg
    # All warming scenarios present
    for s in ["+1.5C", "+2.0C", "+3.0C", "+4.0C", "+5.0C"]:
        assert s in dmg["damages_by_scenario"]
    # GDP loss magnitude grows with warming (larger compounding)
    losses = [abs(v["cumulative_gdp_loss_pct_by_2100"]) for v in dmg["damages_by_scenario"].values()]
    assert losses[-1] > losses[0]


def test_weitzman_tail_risk_returns_expected_structure():
    result = ClimateDamage._weitzman_tail_risk(warming_expected=3.0)
    for key in ("exceedance_probabilities_pct", "expected_damage_quadratic_pct_gdp",
                "expected_damage_exponential_pct_gdp", "p95_damage_quadratic",
                "p95_damage_exponential", "tail_risk_ratio"):
        assert key in result
    # Both expected damages must be positive
    assert result["expected_damage_quadratic_pct_gdp"] > 0
    assert result["expected_damage_exponential_pct_gdp"] > 0
    # P95 damage must exceed mean damage for quadratic spec
    assert result["p95_damage_quadratic"] > result["expected_damage_quadratic_pct_gdp"]


def test_discount_rate_stern_lower_than_nordhaus():
    result = ClimateDamage._discount_rate_analysis(gdp_growth_rate=0.02)
    assert result["stern"]["discount_rate"] < result["nordhaus"]["discount_rate"]
    assert result["scc_sensitivity"]["stern_nordhaus_ratio_50yr"] > 1


def test_approximate_baseline_temp_tropical():
    assert ClimateDamage._approximate_baseline_temp("BGD") == 26.0
    assert ClimateDamage._approximate_baseline_temp("RUS") == 3.0
    assert ClimateDamage._approximate_baseline_temp("DEU") == 12.0
