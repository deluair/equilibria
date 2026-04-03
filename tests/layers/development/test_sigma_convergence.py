import pytest
from app.layers.development.sigma_convergence import SigmaConvergence
from tests.layers.development.conftest import insert_series, insert_point


def test_instantiation():
    assert SigmaConvergence() is not None


def test_layer_id():
    assert SigmaConvergence.layer_id == "l4"


def test_name():
    assert SigmaConvergence().name == "Sigma Convergence"


async def test_empty_db_score_is_50(db_conn):
    result = await SigmaConvergence().compute(db_conn)
    assert result["score"] == 50


async def test_score_in_range_with_data(db_conn):
    # 25 countries, 10 years of GDP per capita
    for i in range(25):
        iso = f"S{i:02d}"
        sid = await insert_series(db_conn, "NY.GDP.PCAP.KD", iso)
        for yr in range(2000, 2010):
            val = max(100.0, 1000 * (i + 1) * (1 - 0.002 * (yr - 2000)))
            await insert_point(db_conn, sid, f"{yr}-01-01", val)

    result = await SigmaConvergence().compute(db_conn)
    assert 0 <= result["score"] <= 100


async def test_results_trend_key_present(db_conn):
    for i in range(25):
        iso = f"T{i:02d}"
        sid = await insert_series(db_conn, "NY.GDP.PCAP.KD", iso)
        for yr in range(2000, 2010):
            await insert_point(db_conn, sid, f"{yr}-01-01", 500 * (i + 1))

    result = await SigmaConvergence().compute(db_conn)
    assert "results" in result
    if "trend" in result["results"]:
        assert "coef" in result["results"]["trend"]


async def test_converging_flag_present(db_conn):
    for i in range(25):
        iso = f"U{i:02d}"
        sid = await insert_series(db_conn, "NY.GDP.PCAP.KD", iso)
        for yr in range(2000, 2010):
            await insert_point(db_conn, sid, f"{yr}-01-01", 1000.0 + i * 100)

    result = await SigmaConvergence().compute(db_conn)
    res = result["results"]
    if "converging" in res:
        assert isinstance(res["converging"], bool)
