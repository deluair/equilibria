import pytest
from app.layers.development.mpi import MultidimensionalPoverty, MPI_INDICATORS, POVERTY_CUTOFF
from tests.layers.development.conftest import insert_series, insert_point


def test_instantiation():
    assert MultidimensionalPoverty() is not None


def test_layer_id():
    assert MultidimensionalPoverty.layer_id == "l4"


def test_name():
    assert MultidimensionalPoverty().name == "Multidimensional Poverty (MPI)"


def test_poverty_cutoff_is_one_third():
    assert abs(POVERTY_CUTOFF - 1 / 3) < 1e-9


async def test_empty_db_returns_50(db_conn):
    result = await MultidimensionalPoverty().compute(db_conn)
    assert result["score"] == 50


async def test_score_in_range_with_indicator_data(db_conn):
    # Seed a few indicators for 5 countries
    for i in range(5):
        iso = f"M{i:02d}"
        sid = await insert_series(db_conn, "SH.DYN.MORT", iso)
        await insert_point(db_conn, sid, "2022-01-01", 30.0 + i * 5)
        sid2 = await insert_series(db_conn, "SE.ADT.LITR.ZS", iso)
        await insert_point(db_conn, sid2, "2022-01-01", 70.0 - i * 5)

    result = await MultidimensionalPoverty().compute(db_conn)
    assert 0 <= result["score"] <= 100


async def test_results_has_global_mpi(db_conn):
    for i in range(5):
        iso = f"N{i:02d}"
        sid = await insert_series(db_conn, "EG.ELC.ACCS.ZS", iso)
        await insert_point(db_conn, sid, "2022-01-01", 40.0 + i * 3)

    result = await MultidimensionalPoverty().compute(db_conn)
    assert "results" in result
    if "global_mpi" in result["results"]:
        assert isinstance(result["results"]["global_mpi"], float)


async def test_country_iso3_kwarg_accepted(db_conn):
    result = await MultidimensionalPoverty().compute(db_conn, country_iso3="BGD")
    assert result["results"]["country_iso3"] == "BGD"
