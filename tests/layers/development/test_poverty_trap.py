import pytest
from app.layers.development.poverty_trap import PovertyTrap
from tests.layers.development.conftest import insert_series, insert_point


def test_instantiation():
    assert PovertyTrap() is not None


def test_layer_id():
    assert PovertyTrap.layer_id == "l4"


def test_name():
    assert PovertyTrap().name == "Poverty Trap Detection"


async def test_empty_db_returns_50(db_conn):
    result = await PovertyTrap().compute(db_conn)
    assert result["score"] == 50


async def test_score_in_range_with_data(db_conn):
    for i in range(25):
        iso = f"P{i:02d}"
        sid = await insert_series(db_conn, "NY.GDP.PCAP.KD", iso)
        await insert_point(db_conn, sid, "2020-01-01", 500.0 * (i + 1))

    result = await PovertyTrap().compute(db_conn)
    assert 0 <= result["score"] <= 100


async def test_results_has_bimodality(db_conn):
    for i in range(25):
        iso = f"Q{i:02d}"
        sid = await insert_series(db_conn, "NY.GDP.PCAP.KD", iso)
        # Two clusters: low and high income
        val = 300.0 if i < 12 else 15000.0
        await insert_point(db_conn, sid, "2020-01-01", val)

    result = await PovertyTrap().compute(db_conn)
    assert "results" in result
    assert "bimodality" in result["results"]


async def test_target_country_club(db_conn):
    for i in range(25):
        iso = f"R{i:02d}"
        sid = await insert_series(db_conn, "NY.GDP.PCAP.KD", iso)
        val = 300.0 if i < 12 else 15000.0
        await insert_point(db_conn, sid, "2020-01-01", val)

    result = await PovertyTrap().compute(db_conn, country_iso3="R00")
    assert "results" in result
    assert "country_iso3" in result["results"]
    assert result["results"]["country_iso3"] == "R00"
