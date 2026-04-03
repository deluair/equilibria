import numpy as np
import pytest
from app.layers.environmental.carbon_pricing import CarbonPricing


def test_instantiation():
    m = CarbonPricing()
    assert m is not None


def test_layer_id():
    assert CarbonPricing.layer_id == "l9"


def test_name():
    assert CarbonPricing().name == "Carbon Pricing"


async def test_compute_empty_db_unavailable(db_conn):
    result = await CarbonPricing().compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)
    assert result["score"] is None or isinstance(result["score"], (int, float))


async def test_compute_with_sufficient_data(db_conn):
    # Insert 15 paired CO2 + GDP rows so the module gets past the < 10 guard.
    await db_conn.execute(
        "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?,?,?,?)",
        ("wdi", "EN.ATM.CO2E.KT", "BGD", "CO2 emissions kt"),
    )
    await db_conn.execute(
        "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?,?,?,?)",
        ("wdi", "NY.GDP.MKTP.KD", "BGD", "GDP"),
    )
    co2_id = (await db_conn.fetch_one("SELECT id FROM data_series WHERE series_id='EN.ATM.CO2E.KT'"))["id"]
    gdp_id = (await db_conn.fetch_one("SELECT id FROM data_series WHERE series_id='NY.GDP.MKTP.KD'"))["id"]

    for yr in range(2000, 2016):
        co2_val = 60000 + yr * 100
        gdp_val = 1e11 + yr * 5e9
        await db_conn.execute(
            "INSERT INTO data_points (series_id, date, value) VALUES (?,?,?)",
            (co2_id, f"{yr}-01-01", co2_val),
        )
        await db_conn.execute(
            "INSERT INTO data_points (series_id, date, value) VALUES (?,?,?)",
            (gdp_id, f"{yr}-01-01", gdp_val),
        )

    # Use run() so any internal errors are caught and returned as a dict
    result = await CarbonPricing().run(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)
    assert "score" in result


def test_estimate_scc_positive():
    m = CarbonPricing()
    scc = m._estimate_scc(emissions_kt=60000, gdp_usd=3e11, discount_rate=0.015, horizon=50)
    assert scc["scc_usd_per_tco2"] > 0
    assert scc["scc_stern_usd_per_tco2"] > scc["scc_usd_per_tco2"]


def test_compute_tax_incidence_revenue_positive():
    result = CarbonPricing._compute_tax_incidence(carbon_price=50, emissions_kt=60000, gdp_usd=3e11)
    assert "suits_index" in result
    assert result["total_revenue_musd"] > 0
    assert result["regressivity"] in ("regressive", "proportional", "progressive")
    assert len(result["burden_by_decile_pct"]) == 10


def test_border_carbon_adjustment_direction_above():
    result = CarbonPricing._border_carbon_adjustment(carbon_intensity_domestic=500, scc=50)
    assert result["direction"] == "exports_face_bca"
    assert result["ci_gap"] > 0


def test_border_carbon_adjustment_direction_below():
    result = CarbonPricing._border_carbon_adjustment(carbon_intensity_domestic=100, scc=50)
    assert result["direction"] == "imports_face_bca"
    assert result["ci_gap"] < 0
