import pytest
from app.layers.health.nutrition import NutritionEconomics


def test_instantiation():
    m = NutritionEconomics()
    assert m is not None


def test_layer_id():
    m = NutritionEconomics()
    assert m.layer_id == "l8"


def test_name():
    m = NutritionEconomics()
    assert m.name == "Nutrition Economics"


async def test_compute_empty_db_returns_dict(db_conn):
    m = NutritionEconomics()
    result = await m.compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)


async def test_compute_empty_db_fallback_score(db_conn):
    m = NutritionEconomics()
    result = await m.compute(db_conn, country_iso3="BGD")
    assert "score" in result
    # empty DB: stunt_rows is falsy but gdppc_rows also falsy -> error path
    assert result["score"] == 50


async def test_compute_no_country_iso3(db_conn):
    m = NutritionEconomics()
    result = await m.compute(db_conn)
    assert isinstance(result, dict)
    assert "score" in result


async def test_compute_results_error_on_empty_db(db_conn):
    m = NutritionEconomics()
    result = await m.compute(db_conn, country_iso3="BGD")
    assert "results" in result
    assert "error" in result["results"]


async def test_compute_results_keys_present_with_data(db_conn):
    # Seed minimal stunting + GDP rows so compute() passes the guard
    for sid_str, name, val in [
        ("SH.STA.STNT.ZS", "Stunting", 30.0),
        ("NY.GDP.PCAP.KD", "GDP pc", 1800.0),
    ]:
        await db_conn.execute(
            "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?, ?, ?, ?)",
            ("WDI", sid_str, "BGD", name),
        )
        row = await db_conn.fetch_one(f"SELECT id FROM data_series WHERE series_id='{sid_str}'")
        await db_conn.execute(
            "INSERT INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
            (row["id"], "2019-01-01", val),
        )
    m = NutritionEconomics()
    result = await m.compute(db_conn, country_iso3="BGD")
    assert "results" in result
    r = result["results"]
    assert "income_nutrition_elasticity" in r
    assert "malnutrition" in r
    assert "deficiency_costs" in r
    assert "intervention_cost_effectiveness" in r
    assert "intervention_priority" in r
    assert "country_iso3" in r


async def test_intervention_cost_effectiveness_always_populated_with_data(db_conn):
    # Seed stunting + GDP; intervention dict is hardcoded so always returned
    for sid_str, name, val in [
        ("SH.STA.STNT.ZS", "Stunting", 30.0),
        ("NY.GDP.PCAP.KD", "GDP pc", 1800.0),
    ]:
        await db_conn.execute(
            "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?, ?, ?, ?)",
            ("WDI", sid_str, "BGD", name),
        )
        row = await db_conn.fetch_one(f"SELECT id FROM data_series WHERE series_id='{sid_str}'")
        await db_conn.execute(
            "INSERT INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
            (row["id"], "2019-01-01", val),
        )
    m = NutritionEconomics()
    result = await m.compute(db_conn, country_iso3="BGD")
    r = result["results"]
    assert r["intervention_cost_effectiveness"] is not None
    assert "vitamin_a_supplementation" in r["intervention_cost_effectiveness"]
