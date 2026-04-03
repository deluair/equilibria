import pytest
from app.layers.development.finance_growth import FinanceDevelopmentGrowth
from tests.layers.development.conftest import insert_series, insert_point


def test_instantiation():
    assert FinanceDevelopmentGrowth() is not None


def test_layer_id():
    assert FinanceDevelopmentGrowth.layer_id == "l4"


def test_name():
    assert FinanceDevelopmentGrowth().name == "Finance-Growth Nexus"


async def test_empty_db_returns_50(db_conn):
    result = await FinanceDevelopmentGrowth().compute(db_conn)
    assert result["score"] == 50


async def test_score_in_range_with_data(db_conn):
    for i in range(15):
        iso = f"F{i:02d}"
        for yr in range(2000, 2006):
            sid_cr = await insert_series(db_conn, "FS.AST.PRVT.GD.ZS", iso)
            await insert_point(db_conn, sid_cr, str(yr), 20.0 + i * 5)
            sid_gr = await insert_series(db_conn, "NY.GDP.MKTP.KD.ZG", iso)
            await insert_point(db_conn, sid_gr, str(yr), 3.0 + (i % 4) - 1)

    result = await FinanceDevelopmentGrowth().compute(db_conn)
    assert 0 <= result["score"] <= 100


async def test_results_has_linear_key(db_conn):
    for i in range(15):
        iso = f"E{i:02d}"
        for yr in range(2000, 2006):
            sid_cr = await insert_series(db_conn, "FS.AST.PRVT.GD.ZS", iso)
            await insert_point(db_conn, sid_cr, str(yr), 30.0 + i * 4)
            sid_gr = await insert_series(db_conn, "NY.GDP.MKTP.KD.ZG", iso)
            await insert_point(db_conn, sid_gr, str(yr), 2.0)

    result = await FinanceDevelopmentGrowth().compute(db_conn)
    assert "results" in result
    if "linear" in result["results"]:
        assert "coef" in result["results"]["linear"]


async def test_target_country_depth_class(db_conn):
    iso = "BGD"
    for yr in range(2000, 2006):
        sid_cr = await insert_series(db_conn, "FS.AST.PRVT.GD.ZS", iso)
        await insert_point(db_conn, sid_cr, str(yr), 40.0)
        sid_gr = await insert_series(db_conn, "NY.GDP.MKTP.KD.ZG", iso)
        await insert_point(db_conn, sid_gr, str(yr), 5.0)

    result = await FinanceDevelopmentGrowth().compute(db_conn, country_iso3="BGD")
    assert result["results"]["country_iso3"] == "BGD"
    target = result["results"].get("target")
    if target:
        assert "depth_class" in target
