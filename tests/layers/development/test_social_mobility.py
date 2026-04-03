import pytest
from app.layers.development.social_mobility import SocialMobility
from tests.layers.development.conftest import insert_series, insert_point


def test_instantiation():
    assert SocialMobility() is not None


def test_layer_id():
    assert SocialMobility.layer_id == "l4"


def test_name():
    assert SocialMobility().name == "Social Mobility"


async def test_empty_db_returns_50(db_conn):
    result = await SocialMobility().compute(db_conn)
    assert result["score"] == 50


async def test_score_in_range_with_gini_data(db_conn):
    for i in range(20):
        iso = f"SM{i:02d}"
        sid_g = await insert_series(db_conn, "SI.POV.GINI", iso)
        await insert_point(db_conn, sid_g, "2022-01-01", 25.0 + i * 2)
        sid_b20 = await insert_series(db_conn, "SI.DST.FRST.20", iso)
        await insert_point(db_conn, sid_b20, "2022-01-01", 10.0 - i * 0.3)

    result = await SocialMobility().compute(db_conn)
    assert 0 <= result["score"] <= 100


async def test_results_has_great_gatsby_key(db_conn):
    for i in range(20):
        iso = f"SN{i:02d}"
        sid_g = await insert_series(db_conn, "SI.POV.GINI", iso)
        await insert_point(db_conn, sid_g, "2022-01-01", 30.0 + i)
        sid_al = await insert_series(db_conn, "SE.ADT.LITR.ZS", iso)
        await insert_point(db_conn, sid_al, "2022-01-01", 85.0 - i)
        sid_yl = await insert_series(db_conn, "SE.ADT.1524.LT.ZS", iso)
        await insert_point(db_conn, sid_yl, "2022-01-01", 95.0 - i * 0.5)
        sid_b20 = await insert_series(db_conn, "SI.DST.FRST.20", iso)
        await insert_point(db_conn, sid_b20, "2022-01-01", 8.0 - i * 0.1)

    result = await SocialMobility().compute(db_conn)
    assert "results" in result
    assert "great_gatsby_curve" in result["results"]


async def test_target_analysis_country_iso3(db_conn):
    iso = "BGD"
    sid_g = await insert_series(db_conn, "SI.POV.GINI", iso)
    await insert_point(db_conn, sid_g, "2022-01-01", 32.4)
    sid_b20 = await insert_series(db_conn, "SI.DST.FRST.20", iso)
    await insert_point(db_conn, sid_b20, "2022-01-01", 8.1)

    result = await SocialMobility().compute(db_conn, country_iso3="BGD")
    assert result["results"]["country_iso3"] == "BGD"
    target = result["results"].get("target")
    if target:
        assert "gini" in target
