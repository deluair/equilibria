import pytest
from app.layers.methods.survival_analysis import SurvivalAnalysis


def test_instantiation():
    model = SurvivalAnalysis()
    assert model is not None


def test_layer_id():
    model = SurvivalAnalysis()
    assert model.layer_id == "l18"


def test_name():
    model = SurvivalAnalysis()
    assert model.name == "Survival Analysis"


async def test_compute_empty_db_returns_dict(db_conn):
    model = SurvivalAnalysis()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)


async def test_compute_empty_db_has_score(db_conn):
    model = SurvivalAnalysis()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert "score" in result


async def test_compute_empty_db_score_none_or_numeric(db_conn):
    model = SurvivalAnalysis()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert result["score"] is None or isinstance(result["score"], (int, float))


async def test_run_wraps_no_exception(db_conn):
    model = SurvivalAnalysis()
    result = await model.run(db_conn, country_iso3="BGD")
    assert "layer_id" in result


async def test_compute_different_duration_type(db_conn):
    model = SurvivalAnalysis()
    result = await model.compute(db_conn, country_iso3="BGD", duration_type="firm_exit")
    assert isinstance(result, dict)
    assert "score" in result
