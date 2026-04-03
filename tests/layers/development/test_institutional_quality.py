import pytest
from app.layers.development.institutional_quality import InstitutionalQuality
from tests.layers.development.conftest import insert_series, insert_point, insert_country


def test_instantiation():
    assert InstitutionalQuality() is not None


def test_layer_id():
    assert InstitutionalQuality.layer_id == "l4"


def test_name():
    assert InstitutionalQuality().name == "Institutional Quality"


async def test_empty_db_returns_50(db_conn):
    result = await InstitutionalQuality().compute(db_conn)
    assert result["score"] == 50


async def test_score_in_range_with_data(db_conn):
    isos = [f"I{i:02d}" for i in range(25)]
    for i, iso in enumerate(isos):
        sid_rl = await insert_series(db_conn, "RL.EST", iso)
        await insert_point(db_conn, sid_rl, "2022-01-01", -2.5 + i * 0.2)
        sid_gdp = await insert_series(db_conn, "NY.GDP.PCAP.KD", iso)
        await insert_point(db_conn, sid_gdp, "2022-01-01", 1000.0 * (i + 1))

    result = await InstitutionalQuality().compute(db_conn)
    assert 0 <= result["score"] <= 100


async def test_ols_key_present(db_conn):
    isos = [f"J{i:02d}" for i in range(25)]
    for i, iso in enumerate(isos):
        sid_rl = await insert_series(db_conn, "RL.EST", iso)
        await insert_point(db_conn, sid_rl, "2022-01-01", -1.5 + i * 0.12)
        sid_gdp = await insert_series(db_conn, "NY.GDP.PCAP.KD", iso)
        await insert_point(db_conn, sid_gdp, "2022-01-01", 2000.0 * (i + 1))

    result = await InstitutionalQuality().compute(db_conn)
    assert "results" in result
    if "ols" in result["results"]:
        assert "coef" in result["results"]["ols"]
        assert "n_obs" in result["results"]["ols"]


async def test_target_rank_with_country_iso3(db_conn):
    isos = [f"K{i:02d}" for i in range(25)]
    for i, iso in enumerate(isos):
        sid_rl = await insert_series(db_conn, "RL.EST", iso)
        await insert_point(db_conn, sid_rl, "2022-01-01", -1.0 + i * 0.08)
        sid_gdp = await insert_series(db_conn, "NY.GDP.PCAP.KD", iso)
        await insert_point(db_conn, sid_gdp, "2022-01-01", 500.0 * (i + 1))

    result = await InstitutionalQuality().compute(db_conn, country_iso3="K00")
    assert "results" in result
    assert result["results"]["country_iso3"] == "K00"
