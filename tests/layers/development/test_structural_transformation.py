import pytest
from app.layers.development.structural_transformation import StructuralTransformation
from tests.layers.development.conftest import insert_series, insert_point


def test_instantiation():
    assert StructuralTransformation() is not None


def test_layer_id():
    assert StructuralTransformation.layer_id == "l4"


def test_name():
    assert StructuralTransformation().name == "Structural Transformation"


async def test_empty_db_returns_50(db_conn):
    result = await StructuralTransformation().compute(db_conn)
    assert result["score"] == 50


async def test_score_in_range_with_sector_data(db_conn):
    iso = "BGD"
    for yr in range(2000, 2010):
        for series_id, val in [
            ("NV.AGR.TOTL.ZS", 20.0),
            ("NV.IND.MANF.ZS", 18.0),
            ("NV.IND.TOTL.ZS", 30.0),
            ("NV.SRV.TOTL.ZS", 50.0),
        ]:
            sid = await insert_series(db_conn, series_id, iso)
            await insert_point(db_conn, sid, str(yr), val)

    result = await StructuralTransformation().compute(db_conn, country_iso3="BGD")
    assert 0 <= result["score"] <= 100


async def test_results_has_n_countries(db_conn):
    for i in range(3):
        iso = f"ST{i:02d}"
        for yr in range(2000, 2006):
            sid = await insert_series(db_conn, "NV.AGR.TOTL.ZS", iso)
            await insert_point(db_conn, sid, str(yr), 15.0 + i)

    result = await StructuralTransformation().compute(db_conn)
    assert "results" in result
    assert "n_countries" in result["results"]


async def test_premature_deindustrialization_key(db_conn):
    iso = "BGD"
    for yr in range(2000, 2010):
        sid = await insert_series(db_conn, "NV.IND.MANF.ZS", iso)
        await insert_point(db_conn, sid, str(yr), 14.0)
    sid_gdp = await insert_series(db_conn, "NY.GDP.PCAP.KD", iso)
    await insert_point(db_conn, sid_gdp, "2009-01-01", 800.0)

    result = await StructuralTransformation().compute(db_conn, country_iso3="BGD")
    assert "results" in result
    assert "premature_deindustrialization" in result["results"]
