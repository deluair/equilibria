import pytest
from app.layers.development.solow_residual import SolowResidual
from tests.layers.development.conftest import insert_series, insert_point


def test_instantiation():
    assert SolowResidual() is not None


def test_layer_id():
    assert SolowResidual.layer_id == "l4"


def test_name():
    assert SolowResidual().name == "Solow Residual (TFP)"


async def test_empty_db_returns_50(db_conn):
    result = await SolowResidual().compute(db_conn)
    assert result["score"] == 50


async def test_score_in_range_with_data(db_conn):
    for series_id in ("NY.GDP.MKTP.KD", "SL.TLF.TOTL.IN"):
        sid = await insert_series(db_conn, series_id, "BGD")
        for yr in range(2000, 2010):
            await insert_point(db_conn, sid, str(yr), 1e9 * (1.05 ** (yr - 2000)))

    result = await SolowResidual().compute(db_conn, country_iso3="BGD")
    assert 0 <= result["score"] <= 100


async def test_alpha_kwarg_accepted(db_conn):
    result = await SolowResidual().compute(db_conn, alpha=0.4)
    assert isinstance(result, dict)
    assert "score" in result


async def test_results_has_n_countries(db_conn):
    for series_id in ("NY.GDP.MKTP.KD", "SL.TLF.TOTL.IN"):
        sid = await insert_series(db_conn, series_id, "USA")
        for yr in range(2005, 2012):
            await insert_point(db_conn, sid, str(yr), 2e10 * (1.03 ** (yr - 2005)))

    result = await SolowResidual().compute(db_conn)
    assert "results" in result
    if "n_countries" in result["results"]:
        assert result["results"]["n_countries"] >= 0
