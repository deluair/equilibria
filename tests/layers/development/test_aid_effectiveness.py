import pytest
from app.layers.development.aid_effectiveness import AidEffectiveness
from tests.layers.development.conftest import insert_series, insert_point


def test_instantiation():
    assert AidEffectiveness() is not None


def test_layer_id():
    assert AidEffectiveness.layer_id == "l4"


def test_name():
    assert AidEffectiveness().name == "Aid Effectiveness"


async def test_empty_db_returns_50(db_conn):
    result = await AidEffectiveness().compute(db_conn)
    assert result["score"] == 50


async def test_score_in_range_with_data(db_conn):
    isos = [f"A{i:02d}" for i in range(15)]
    for i, iso in enumerate(isos):
        for yr in range(2000, 2010):
            # ODA % of GNI
            sid_oda = await insert_series(db_conn, "DT.ODA.ODAT.GN.ZS", iso)
            await insert_point(db_conn, sid_oda, str(yr), 2.0 + i * 0.5)
            # GDP growth
            sid_gdp = await insert_series(db_conn, "NY.GDP.MKTP.KD.ZG", iso)
            await insert_point(db_conn, sid_gdp, str(yr), 3.0 + (i % 4) - 2)
            # Inflation
            sid_inf = await insert_series(db_conn, "FP.CPI.TOTL.ZG", iso)
            await insert_point(db_conn, sid_inf, str(yr), 5.0 + i)
            # Trade openness
            sid_tr = await insert_series(db_conn, "NE.TRD.GNFS.ZS", iso)
            await insert_point(db_conn, sid_tr, str(yr), 40.0 + i * 2)

    result = await AidEffectiveness().compute(db_conn)
    assert 0 <= result["score"] <= 100


async def test_results_has_baseline_key(db_conn):
    isos = [f"B{i:02d}" for i in range(15)]
    for i, iso in enumerate(isos):
        for yr in range(2005, 2012):
            sid_oda = await insert_series(db_conn, "DT.ODA.ODAT.GN.ZS", iso)
            await insert_point(db_conn, sid_oda, str(yr), 3.0)
            sid_gdp = await insert_series(db_conn, "NY.GDP.MKTP.KD.ZG", iso)
            await insert_point(db_conn, sid_gdp, str(yr), 2.5)
            sid_inf = await insert_series(db_conn, "FP.CPI.TOTL.ZG", iso)
            await insert_point(db_conn, sid_inf, str(yr), 6.0)
            sid_tr = await insert_series(db_conn, "NE.TRD.GNFS.ZS", iso)
            await insert_point(db_conn, sid_tr, str(yr), 50.0)

    result = await AidEffectiveness().compute(db_conn)
    assert "results" in result
    if "baseline" in result["results"]:
        assert "aid_coef" in result["results"]["baseline"]


async def test_country_iso3_kwarg_accepted(db_conn):
    result = await AidEffectiveness().compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)
    assert "score" in result
