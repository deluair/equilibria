import pytest
from app.layers.development.hdi_decomposition import HDIDecomposition, _dimension_index
from tests.layers.development.conftest import insert_series, insert_point


def test_instantiation():
    assert HDIDecomposition() is not None


def test_layer_id():
    assert HDIDecomposition.layer_id == "l4"


def test_name():
    assert HDIDecomposition().name == "HDI Decomposition"


def test_dimension_index_bounds():
    assert _dimension_index(20.0, 20.0, 85.0) == 0.0
    assert _dimension_index(85.0, 20.0, 85.0) == 1.0
    assert 0.0 < _dimension_index(50.0, 20.0, 85.0) < 1.0


async def test_empty_db_returns_50(db_conn):
    result = await HDIDecomposition().compute(db_conn)
    assert result["score"] == 50


async def test_score_in_range_with_data(db_conn):
    isos = [f"H{i:02d}" for i in range(15)]
    for i, iso in enumerate(isos):
        sid_le = await insert_series(db_conn, "SP.DYN.LE00.IN", iso)
        await insert_point(db_conn, sid_le, "2022-01-01", 50.0 + i * 2)
        sid_gni = await insert_series(db_conn, "NY.GNP.PCAP.PP.KD", iso)
        await insert_point(db_conn, sid_gni, "2022-01-01", 1000.0 * (i + 1))

    result = await HDIDecomposition().compute(db_conn)
    assert 0 <= result["score"] <= 100


async def test_results_has_global_stats(db_conn):
    for i in range(5):
        iso = f"G{i:02d}"
        sid_le = await insert_series(db_conn, "SP.DYN.LE00.IN", iso)
        await insert_point(db_conn, sid_le, "2021-01-01", 65.0 + i)
        sid_gni = await insert_series(db_conn, "NY.GNP.PCAP.PP.KD", iso)
        await insert_point(db_conn, sid_gni, "2021-01-01", 5000.0 + i * 1000)

    result = await HDIDecomposition().compute(db_conn)
    assert "results" in result
    if "global" in result["results"]:
        assert "mean_hdi" in result["results"]["global"]


async def test_target_country_iso3_preserved(db_conn):
    result = await HDIDecomposition().compute(db_conn, country_iso3="BGD")
    assert result["results"]["country_iso3"] == "BGD"
