import numpy as np
import pytest
from app.layers.environmental.ekc import EnvironmentalKuznetsCurve


def test_instantiation():
    assert EnvironmentalKuznetsCurve() is not None


def test_layer_id():
    assert EnvironmentalKuznetsCurve.layer_id == "l9"


def test_name():
    assert EnvironmentalKuznetsCurve().name == "Environmental Kuznets Curve"


async def test_compute_empty_db_returns_score(db_conn):
    result = await EnvironmentalKuznetsCurve().compute(db_conn)
    assert isinstance(result, dict)
    assert "score" in result


async def test_compute_with_panel_data(db_conn):
    # Insert CO2 per capita and GDP per capita for 5 countries x 10 years each
    countries = ["BGD", "IND", "CHN", "BRA", "USA"]
    for iso in countries:
        await db_conn.execute(
            "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?,?,?,?)",
            ("wdi", "EN.ATM.CO2E.PC", iso, f"CO2 pc {iso}"),
        )
        await db_conn.execute(
            "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?,?,?,?)",
            ("wdi", "NY.GDP.PCAP.KD", iso, f"GDP pc {iso}"),
        )
    for iso, base_co2, base_gdp in [
        ("BGD", 0.5, 1500), ("IND", 1.5, 2000), ("CHN", 5.0, 8000),
        ("BRA", 2.2, 10000), ("USA", 15.0, 50000),
    ]:
        co2_id = (await db_conn.fetch_one(
            "SELECT id FROM data_series WHERE series_id='EN.ATM.CO2E.PC' AND country_iso3=?", (iso,)
        ))["id"]
        gdp_id = (await db_conn.fetch_one(
            "SELECT id FROM data_series WHERE series_id='NY.GDP.PCAP.KD' AND country_iso3=?", (iso,)
        ))["id"]
        for i in range(10):
            yr = 2010 + i
            await db_conn.execute(
                "INSERT INTO data_points (series_id, date, value) VALUES (?,?,?)",
                (co2_id, f"{yr}-01-01", base_co2 * (1 + i * 0.03)),
            )
            await db_conn.execute(
                "INSERT INTO data_points (series_id, date, value) VALUES (?,?,?)",
                (gdp_id, f"{yr}-01-01", base_gdp * (1 + i * 0.05)),
            )

    result = await EnvironmentalKuznetsCurve().compute(db_conn, pollutant="co2_pc")
    assert isinstance(result, dict)
    assert "score" in result
    assert "quadratic_fe" in result
    if result["score"] is not None:
        assert 0 <= result["score"] <= 100


def test_demean_removes_group_means():
    groups = np.array([0, 0, 0, 1, 1, 1])
    arr = np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0])
    (demeaned,) = EnvironmentalKuznetsCurve._demean(arr, groups=groups)
    # Group 0 mean = 2.0, group 1 mean = 5.0
    np.testing.assert_allclose(demeaned[:3], [-1.0, 0.0, 1.0])
    np.testing.assert_allclose(demeaned[3:], [-1.0, 0.0, 1.0])


def test_hc1_se_shape():
    X = np.column_stack([np.ones(20), np.linspace(1, 20, 20)])
    resid = np.random.default_rng(42).normal(0, 1, 20)
    se = EnvironmentalKuznetsCurve._hc1_se(X, resid, n=20, n_groups=3)
    assert se.shape == (2,)
    assert np.all(se >= 0)
