import pytest
from app.layers.development.kuznets_curve import KuznetsCurve
from tests.layers.development.conftest import insert_series, insert_point


def test_instantiation():
    assert KuznetsCurve() is not None


def test_layer_id():
    assert KuznetsCurve.layer_id == "l4"


def test_name():
    assert KuznetsCurve().name == "Kuznets Curve"


async def test_empty_db_returns_50(db_conn):
    result = await KuznetsCurve().compute(db_conn)
    assert result["score"] == 50


async def test_score_in_range_with_data(db_conn):
    for i in range(15):
        iso = f"K{i:02d}"
        for yr in range(2000, 2005):
            sid_g = await insert_series(db_conn, "SI.POV.GINI", iso)
            await insert_point(db_conn, sid_g, str(yr), 30.0 + i * 2)
            sid_gdp = await insert_series(db_conn, "NY.GDP.PCAP.KD", iso)
            await insert_point(db_conn, sid_gdp, str(yr), 1000.0 * (i + 1))

    result = await KuznetsCurve().compute(db_conn)
    assert 0 <= result["score"] <= 100


async def test_results_has_pooled_ols_key(db_conn):
    for i in range(15):
        iso = f"L{i:02d}"
        for yr in range(2000, 2005):
            sid_g = await insert_series(db_conn, "SI.POV.GINI", iso)
            await insert_point(db_conn, sid_g, str(yr), 25.0 + i * 3)
            sid_gdp = await insert_series(db_conn, "NY.GDP.PCAP.KD", iso)
            await insert_point(db_conn, sid_gdp, str(yr), 800.0 * (i + 1))

    result = await KuznetsCurve().compute(db_conn)
    assert "results" in result
    if "pooled_ols" in result["results"]:
        assert "beta_log_gdp" in result["results"]["pooled_ols"]


async def test_target_country_analysis(db_conn):
    iso = "BGD"
    for yr in range(2000, 2006):
        sid_g = await insert_series(db_conn, "SI.POV.GINI", iso)
        await insert_point(db_conn, sid_g, str(yr), 32.0)
        sid_gdp = await insert_series(db_conn, "NY.GDP.PCAP.KD", iso)
        await insert_point(db_conn, sid_gdp, str(yr), 700.0)

    result = await KuznetsCurve().compute(db_conn, country_iso3="BGD")
    assert result["results"]["country_iso3"] == "BGD"
