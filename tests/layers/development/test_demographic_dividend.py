import pytest
from app.layers.development.demographic_dividend import DemographicDividend
from tests.layers.development.conftest import insert_series, insert_point


def test_instantiation():
    assert DemographicDividend() is not None


def test_layer_id():
    assert DemographicDividend.layer_id == "l4"


def test_name():
    assert DemographicDividend().name == "Demographic Dividend"


async def test_empty_db_returns_50(db_conn):
    result = await DemographicDividend().compute(db_conn)
    assert result["score"] == 50


async def test_score_in_range_with_data(db_conn):
    for i in range(12):
        iso = f"DD{i:02d}"
        for yr in range(2000, 2008):
            sid_dep = await insert_series(db_conn, "SP.POP.DPND", iso)
            # Vary both level and change across countries/years
            await insert_point(db_conn, sid_dep, str(yr), 50.0 + i * 2.0 - (yr - 2000) * (0.5 + i * 0.1))
            sid_gdp = await insert_series(db_conn, "NY.GDP.PCAP.KD.ZG", iso)
            await insert_point(db_conn, sid_gdp, str(yr), 2.0 + i * 0.3)

    result = await DemographicDividend().compute(db_conn)
    assert 0 <= result["score"] <= 100


async def test_first_dividend_key(db_conn):
    for i in range(12):
        iso = f"DE{i:02d}"
        for yr in range(2000, 2008):
            sid_dep = await insert_series(db_conn, "SP.POP.DPND", iso)
            await insert_point(db_conn, sid_dep, str(yr), 55.0 + i * 1.5 - (yr - 2000) * (0.3 + i * 0.05))
            sid_gdp = await insert_series(db_conn, "NY.GDP.PCAP.KD.ZG", iso)
            await insert_point(db_conn, sid_gdp, str(yr), 1.5 + i * 0.4)

    result = await DemographicDividend().compute(db_conn)
    assert "results" in result
    if "first_dividend" in result["results"] and result["results"]["first_dividend"]:
        assert "dep_change_coef" in result["results"]["first_dividend"]


async def test_target_phase_key(db_conn):
    iso = "BGD"
    for yr in range(2000, 2012):
        sid_dep = await insert_series(db_conn, "SP.POP.DPND", iso)
        await insert_point(db_conn, sid_dep, str(yr), 75.0 - (yr - 2000))
        sid_gdp = await insert_series(db_conn, "NY.GDP.PCAP.KD.ZG", iso)
        await insert_point(db_conn, sid_gdp, str(yr), 5.0)

    result = await DemographicDividend().compute(db_conn, country_iso3="BGD")
    assert result["results"]["country_iso3"] == "BGD"
    target = result["results"].get("target")
    if target:
        assert "phase" in target
