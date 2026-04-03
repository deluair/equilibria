import pytest
from app.layers.trade.export_survival import ExportSurvival


def test_instantiation():
    model = ExportSurvival()
    assert model is not None


def test_layer_id():
    model = ExportSurvival()
    assert model.layer_id == "l1"


def test_name():
    model = ExportSurvival()
    assert model.name == "Export Survival Analysis"


async def test_run_empty_db_returns_dict(db_conn):
    model = ExportSurvival()
    result = await model.run(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)


async def test_run_empty_db_has_score_key(db_conn):
    model = ExportSurvival()
    result = await model.run(db_conn, country_iso3="BGD")
    assert "score" in result


async def test_run_empty_db_score_is_none_or_numeric(db_conn):
    model = ExportSurvival()
    result = await model.run(db_conn, country_iso3="BGD")
    assert result["score"] is None or isinstance(result["score"], (int, float))


async def test_run_no_exception_on_empty_data(db_conn):
    model = ExportSurvival()
    result = await model.run(db_conn, country_iso3="BGD")
    assert "score" in result


async def test_run_different_country(db_conn):
    model = ExportSurvival()
    result = await model.run(db_conn, country_iso3="KHM")
    assert isinstance(result, dict)
    assert "score" in result
