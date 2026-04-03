import pytest
from app.layers.health.pharmaceutical import PharmaceuticalEconomics


def test_instantiation():
    m = PharmaceuticalEconomics()
    assert m is not None


def test_layer_id():
    m = PharmaceuticalEconomics()
    assert m.layer_id == "l8"


def test_name():
    m = PharmaceuticalEconomics()
    assert m.name == "Pharmaceutical Economics"


async def test_compute_empty_db_returns_dict(db_conn):
    m = PharmaceuticalEconomics()
    result = await m.compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)


async def test_compute_empty_db_fallback_score(db_conn):
    m = PharmaceuticalEconomics()
    result = await m.compute(db_conn, country_iso3="BGD")
    assert "score" in result
    assert result["score"] == 50


async def test_compute_no_country_iso3(db_conn):
    m = PharmaceuticalEconomics()
    result = await m.compute(db_conn)
    assert isinstance(result, dict)
    assert "score" in result


async def test_compute_results_error_on_empty_db(db_conn):
    m = PharmaceuticalEconomics()
    result = await m.compute(db_conn, country_iso3="BGD")
    assert "results" in result
    assert "error" in result["results"]


async def test_compute_results_keys_present_with_data(db_conn):
    # Seed minimal hepc + GDP rows so compute() passes the guard
    for sid_str, name in [("SH.XPD.CHEX.PC.CD", "Health exp pc"), ("NY.GDP.PCAP.KD", "GDP pc")]:
        await db_conn.execute(
            "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?, ?, ?, ?)",
            ("WDI", sid_str, "BGD", name),
        )
        row = await db_conn.fetch_one(f"SELECT id FROM data_series WHERE series_id='{sid_str}'")
        await db_conn.execute(
            "INSERT INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
            (row["id"], "2019-01-01", 200.0),
        )
    m = PharmaceuticalEconomics()
    result = await m.compute(db_conn, country_iso3="BGD")
    assert "results" in result
    r = result["results"]
    assert "drug_price_index" in r
    assert "generic_entry_effects" in r
    assert "patent_cliff" in r
    assert "trips_flexibility" in r
    assert "country_iso3" in r
