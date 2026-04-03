import numpy as np
import pytest
from app.layers.environmental.green_growth import GreenGrowth


def test_instantiation():
    assert GreenGrowth() is not None


def test_layer_id():
    assert GreenGrowth.layer_id == "l9"


def test_name():
    assert GreenGrowth().name == "Green Growth"


async def test_compute_empty_db_unavailable(db_conn):
    result = await GreenGrowth().compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)
    assert result.get("score") is None or isinstance(result["score"], (int, float))


async def test_compute_with_co2_and_gdp_data(db_conn):
    for sid, sname in [("EN.ATM.CO2E.KT", "CO2 kt"), ("NY.GDP.MKTP.KD", "GDP")]:
        await db_conn.execute(
            "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?,?,?,?)",
            ("wdi", sid, "BGD", sname),
        )
    co2_id = (await db_conn.fetch_one("SELECT id FROM data_series WHERE series_id='EN.ATM.CO2E.KT'"))["id"]
    gdp_id = (await db_conn.fetch_one("SELECT id FROM data_series WHERE series_id='NY.GDP.MKTP.KD'"))["id"]
    for i in range(12):
        yr = 2005 + i
        await db_conn.execute(
            "INSERT INTO data_points (series_id, date, value) VALUES (?,?,?)",
            (co2_id, f"{yr}-01-01", 50000.0 + i * 500),
        )
        await db_conn.execute(
            "INSERT INTO data_points (series_id, date, value) VALUES (?,?,?)",
            (gdp_id, f"{yr}-01-01", 1e11 + i * 5e9),
        )

    result = await GreenGrowth().compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)
    assert "score" in result


def test_compute_decoupling_absolute():
    co2 = {"2000": 100.0, "2001": 95.0, "2002": 90.0, "2003": 85.0, "2004": 80.0}
    gdp = {"2000": 1000.0, "2001": 1050.0, "2002": 1100.0, "2003": 1150.0, "2004": 1200.0}
    result = GreenGrowth._compute_decoupling(co2, gdp)
    assert result["status"] == "absolute_decoupling"
    assert result["latest_elasticity"] < 0


def test_compute_genuine_savings_uses_adj_ts():
    adj_ts = {"2018": 8.5, "2019": 9.0, "2020": 7.5}
    result = GreenGrowth._compute_genuine_savings(
        gns_ts={}, depreciation_ts={}, education_ts={},
        resource_ts={}, co2_ts={}, adj_ts=adj_ts,
    )
    assert result["latest_adj_net_savings_pct"] == 7.5
    assert result["sustainable"] is True


def test_approximate_iwi_natural_capital_share():
    gdp_ts = {"2020": 3e11}
    energy_rent = {"2020": 2.0}
    result = GreenGrowth._approximate_iwi(
        gdp_ts=gdp_ts, forest_rent_ts={}, mineral_rent_ts={}, energy_rent_ts=energy_rent
    )
    assert result["natural_capital_share_pct"] > 0
    assert result["total_inclusive_wealth_usd"] > result["natural_capital_usd"]
