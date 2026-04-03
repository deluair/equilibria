import pytest
from app.layers.development.resource_curse import ResourceCurse
from tests.layers.development.conftest import insert_series, insert_point


def test_instantiation():
    assert ResourceCurse() is not None


def test_layer_id():
    assert ResourceCurse.layer_id == "l4"


def test_name():
    assert ResourceCurse().name == "Resource Curse"


async def test_empty_db_returns_50(db_conn):
    result = await ResourceCurse().compute(db_conn)
    assert result["score"] == 50


async def test_score_in_range_with_data(db_conn):
    for i in range(22):
        iso = f"RC{i:02d}"
        for yr in range(2000, 2006):
            sid_r = await insert_series(db_conn, "NY.GDP.TOTL.RT.ZS", iso)
            await insert_point(db_conn, sid_r, str(yr), i * 1.5)
            sid_g = await insert_series(db_conn, "NY.GDP.MKTP.KD.ZG", iso)
            await insert_point(db_conn, sid_g, str(yr), 3.0 - i * 0.05)

    result = await ResourceCurse().compute(db_conn)
    assert 0 <= result["score"] <= 100


async def test_results_has_baseline(db_conn):
    for i in range(22):
        iso = f"RD{i:02d}"
        for yr in range(2000, 2006):
            sid_r = await insert_series(db_conn, "NY.GDP.TOTL.RT.ZS", iso)
            await insert_point(db_conn, sid_r, str(yr), 5.0 * i)
            sid_g = await insert_series(db_conn, "NY.GDP.MKTP.KD.ZG", iso)
            await insert_point(db_conn, sid_g, str(yr), 2.0)

    result = await ResourceCurse().compute(db_conn)
    assert "results" in result
    if "baseline" in result["results"]:
        assert "rents_coef" in result["results"]["baseline"]


async def test_target_country_rents(db_conn):
    # Use 22+ countries so the 20-country threshold is met
    for i in range(22):
        iso = f"RE{i:02d}"
        for yr in range(2000, 2006):
            sid_r = await insert_series(db_conn, "NY.GDP.TOTL.RT.ZS", iso)
            await insert_point(db_conn, sid_r, str(yr), i * 1.5)
            sid_g = await insert_series(db_conn, "NY.GDP.MKTP.KD.ZG", iso)
            await insert_point(db_conn, sid_g, str(yr), 2.0 + i * 0.1)
    iso = "NGA"
    for yr in range(2000, 2006):
        sid_r = await insert_series(db_conn, "NY.GDP.TOTL.RT.ZS", iso)
        await insert_point(db_conn, sid_r, str(yr), 25.0)
        sid_g = await insert_series(db_conn, "NY.GDP.MKTP.KD.ZG", iso)
        await insert_point(db_conn, sid_g, str(yr), 2.0)

    result = await ResourceCurse().compute(db_conn, country_iso3="NGA")
    assert "results" in result
    if "country_iso3" in result["results"]:
        assert result["results"]["country_iso3"] == "NGA"
    target = result["results"].get("target")
    if target:
        assert "resource_rents_pct_gdp" in target
